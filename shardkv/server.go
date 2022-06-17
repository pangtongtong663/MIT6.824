package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	CONSENSUS_TIMEOUT   = 500 // ms
	CONFIGCHECK_TIMEOUT = 90
	SENDSHARDS_TIMEOUT  = 150
	NShards             = shardctrler.NShards

	GET       = "get"
	PUT       = "put"
	APPEND    = "append"
	MIGRATE   = "migrate"
	NEWCONFIG = "newconfig"
)

type Op struct {
	Operation        string
	Key              string
	Value            string
	ClientId         int64
	RequestId        int
	NewConfig        shardctrler.Config
	MigrateData      []ShardComponent
	MigrateConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead         int32
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	mck                     *shardctrler.Clerk
	kvDB                    []ShardComponent
	waitApplyCh             map[int]chan Op
	lastSSPointRaftLogIndex int

	config         shardctrler.Config
	migratingShard [NShards]bool
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.kvDB = make([]ShardComponent, NShards)
	for shard := 0; shard < NShards; shard++ {
		kv.kvDB[shard] = ShardComponent{ShardIndex: shard, KVDB: make(map[string]string), ClientRequestId: make(map[int64]int)}
	}

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitApplyCh = make(map[int]chan Op)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ReadRaftApplyCommandLoop()
	go kv.PullNewConfigLoop()
	go kv.SendShardToOtherGroupLoop()

	return kv
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shardIndex := key2shard(args.Key)
	isRespons, isAvali := kv.CheckShardState(args.ConfigNum, shardIndex)
	if !isRespons {
		reply.Err = ErrWrongGroup
		return
	}
	if !isAvali {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: GET,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	raftIndex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		_, isLeader := kv.rf.GetState()
		if kv.isRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) && isLeader {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shardIndex := key2shard(args.Key)
	isRespons, isAvali := kv.CheckShardState(args.ConfigNum, shardIndex)
	if !isRespons {
		reply.Err = ErrWrongGroup
		return
	}
	if !isAvali {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	raftIndex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):

		if kv.isRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:

		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return

}

func (kv *ShardKV) CheckShardState(configNum int, shardIndex int) (bool, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num == configNum && kv.config.Shards[shardIndex] == kv.gid, !kv.migratingShard[shardIndex]
}

func (kv *ShardKV) CheckMigrateState(shardComponents []ShardComponent) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, data := range shardComponents {
		if kv.migratingShard[data.ShardIndex] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

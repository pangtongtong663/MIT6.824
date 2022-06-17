package shardctrler

import (
	"6.824/raft"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	CONSENSUS_TIMEOUT = 5000 // ms

	QueryOp = "query"
	JoinOp  = "join"
	LeaveOp = "leave"
	MoveOp  = "move"
)

type ShardCtrler struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int
	configs      []Config // indexed by config num

	waitApplyCh             map[int]chan Op
	lastRequestId           map[int64]int
	lastSSPointRaftLogIndex int
}

type Op struct {
	Operation    string
	ClientId     int64
	RequestId    int
	Num_Query    int
	Servers_Join map[int][]string
	Gids_Leave   []int
	Shard_Move   int
	Gid_Move     int
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitApplyCh = make(map[int]chan Op)
	sc.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sc.readSnapShot(snapshot)
	}

	go sc.ReadRaftApplyCommandLoop()
	return sc
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: JoinOp, ClientId: args.ClientId, RequestId: args.RequestId, Servers_Join: args.Servers}
	raftIndex, _, _ := sc.rf.Start(op)
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
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

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: LeaveOp, ClientId: args.ClientId, RequestId: args.RequestId, Gids_Leave: args.GIDs}
	raftIndex, _, _ := sc.rf.Start(op)
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
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

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: MoveOp, ClientId: args.ClientId, RequestId: args.RequestId, Shard_Move: args.Shard, Gid_Move: args.GID}
	raftIndex, _, _ := sc.rf.Start(op)
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
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

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: QueryOp, ClientId: args.ClientId, RequestId: args.RequestId, Num_Query: args.Num}
	raftIndex, _, _ := sc.rf.Start(op)
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
			reply.Config = sc.ExecQueryOnController(op)
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Config = sc.ExecQueryOnController(op)
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

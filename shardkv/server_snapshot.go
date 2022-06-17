package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

func (kv *ShardKV) GetSnapShotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShot(message.Snapshot)
		kv.lastSSPointRaftLogIndex = message.SnapshotIndex
	}
}

func (kv *ShardKV) MakeSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.config)
	e.Encode(kv.migratingShard)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) ReadSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	d.Decode(&kv.kvDB)
	d.Decode(&kv.config)
	d.Decode(&kv.migratingShard)

}

func (kv *ShardKV) isNeedToSendSnapShotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

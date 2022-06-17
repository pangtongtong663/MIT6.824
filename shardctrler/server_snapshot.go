package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

func (sc *ShardCtrler) MakeSnapShot() []byte {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.lastRequestId)
	return w.Bytes()
}

func (sc *ShardCtrler) IfNeedToSendSnapShotCommand(raftIndex int, factor int) {
	if sc.rf.GetRaftStateSize() > sc.maxraftstate*factor/10 {
		snapshot := sc.MakeSnapShot()
		sc.rf.Snapshot(raftIndex, snapshot)
	}
}

func (sc *ShardCtrler) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&sc.configs)
	d.Decode(&sc.lastRequestId)
}

func (sc *ShardCtrler) GetSnapShotFromRaft(message raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		sc.readSnapShot(message.Snapshot)
		sc.lastSSPointRaftLogIndex = message.SnapshotIndex
	}
}

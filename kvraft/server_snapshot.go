package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

func (kv *KVServer) makeSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequestId)
	return w.Bytes()
}

func (kv *KVServer) isNeedToSendSnapShotCommand(index int, factor int) {
	if kv.rf.RaftStateSize() > kv.maxraftstate*factor/10 {
		snapShot := kv.makeSnapShot()
		kv.rf.Snapshot(index, snapShot)
	}
}

func (kv *KVServer) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvDB)
	d.Decode(&kv.lastRequestId)

}

func (kv *KVServer) getSnapShotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.readSnapShot(message.Snapshot)
		kv.lastSSPointRaftLogIndex = message.SnapshotIndex
	}
}

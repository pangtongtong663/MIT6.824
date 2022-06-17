package kvraft

import "6.824/raft"

func (kv *KVServer) isRequestDuplicated(clientId int64, requestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastRequest, exit := kv.lastRequestId[clientId]
	if !exit {
		return false
	}

	return requestId <= lastRequest
}

func (kv *KVServer) executeGet(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exit := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	return value, exit
}

func (kv *KVServer) executePut(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvDB[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
}

func (kv *KVServer) executeAppend(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exit := kv.kvDB[op.Key]
	if exit {
		kv.kvDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[op.Key] = op.Value
	}
	kv.lastRequestId[op.ClientId] = op.RequestId
}

func (kv *KVServer) SendMessageToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, exit := kv.waitApplyCh[raftIndex]
	if exit {
		ch <- op
	}
	return exit
}

func (kv *KVServer) getCommand(message raft.ApplyMsg) {
	if message.CommandIndex <= kv.lastSSPointRaftLogIndex {
		return
	}

	op := message.Command.(Op)

	if !kv.isRequestDuplicated(op.ClientId, op.RequestId) {
		if op.Ope == "Put" {
			kv.executePut(op)
		}

		if op.Ope == "Append" {
			kv.executeAppend(op)
		}
	}

	if kv.maxraftstate != -1 {
		kv.isNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	kv.SendMessageToWaitChan(op, message.CommandIndex)
}

func (kv *KVServer) readRaftApplyCommandLoop() {
	for message := range kv.applyCh {
		if message.CommandValid {
			kv.getCommand(message)
		}

		if message.SnapshotValid {
			kv.getSnapShotFromRaft(message)
		}
	}
}

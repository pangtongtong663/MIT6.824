package shardkv

import "6.824/raft"

func (kv *ShardKV) ReadRaftApplyCommandLoop() {
	for message := range kv.applyCh {

		if message.CommandValid {
			kv.GetCommandFromRaft(message)
		}

		if message.SnapshotValid {
			kv.GetSnapShotFromRaft(message)
		}

	}
}

func (kv *ShardKV) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= kv.lastSSPointRaftLogIndex {
		return
	}

	if op.Operation == NEWCONFIG {
		kv.ExecuteNewConfigOpOnServer(op)
		if kv.maxraftstate != -1 {
			kv.isNeedToSendSnapShotCommand(message.CommandIndex, 9)
		}
		return
	}

	if op.Operation == MIGRATE {
		kv.ExecuteMigrateShardsOnServer(op)
		if kv.maxraftstate != -1 {
			kv.isNeedToSendSnapShotCommand(message.CommandIndex, 9)
		}
		kv.SendMessageToWaitChan(op, message.CommandIndex)
		return
	}

	if !kv.isRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
		// execute command
		if op.Operation == PUT {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == APPEND {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	if kv.maxraftstate != -1 {
		kv.isNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	kv.SendMessageToWaitChan(op, message.CommandIndex)
}

func (kv *ShardKV) SendMessageToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

func (kv *ShardKV) ExecuteGetOpOnKVDB(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shardIdx := key2shard(op.Key)

	value, exist := kv.kvDB[shardIdx].KVDB[op.Key]
	kv.kvDB[shardIdx].ClientRequestId[op.ClientId] = op.RequestId
	return value, exist
}

func (kv *ShardKV) ExecutePutOpOnKVDB(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shardIdx := key2shard(op.Key)

	kv.kvDB[shardIdx].KVDB[op.Key] = op.Value
	kv.kvDB[shardIdx].ClientRequestId[op.ClientId] = op.RequestId
}

func (kv *ShardKV) ExecuteAppendOpOnKVDB(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shardIdx := key2shard(op.Key)
	value, exist := kv.kvDB[shardIdx].KVDB[op.Key]
	if exist {
		kv.kvDB[shardIdx].KVDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[shardIdx].KVDB[op.Key] = op.Value
	}
	kv.kvDB[shardIdx].ClientRequestId[op.ClientId] = op.RequestId
}

func (kv *ShardKV) ExecuteNewConfigOpOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig := op.NewConfig

	if newConfig.Num != kv.config.Num+1 {
		return
	}

	for i := 0; i < NShards; i++ {
		if kv.migratingShard[i] {
			return
		}
	}

	kv.checkMigratingShard(newConfig.Shards)
	kv.config = newConfig
}

func (kv *ShardKV) ExecuteMigrateShardsOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	config := kv.config
	if op.MigrateConfigNum != config.Num {
		return
	}

	for _, shardData := range op.MigrateData {
		if !kv.migratingShard[shardData.ShardIndex] {
			continue
		}

		kv.migratingShard[shardData.ShardIndex] = false
		kv.kvDB[shardData.ShardIndex] = ShardComponent{
			ShardIndex:      shardData.ShardIndex,
			KVDB:            make(map[string]string),
			ClientRequestId: make(map[int64]int),
		}

		if config.Shards[shardData.ShardIndex] == kv.gid {
			cloneShard(&kv.kvDB[shardData.ShardIndex], shardData)
		}
	}

}

func cloneShard(cloned *ShardComponent, component ShardComponent) {
	for k, v := range component.KVDB {
		cloned.KVDB[k] = v
	}

	for clientId, requestId := range component.ClientRequestId {
		cloned.ClientRequestId[clientId] = requestId
	}
}

func (kv *ShardKV) checkMigratingShard(newShards [NShards]int) {
	oldShards := kv.config.Shards

	for i := 0; i < NShards; i++ {
		if oldShards[i] == kv.gid && newShards[i] != kv.gid {
			if newShards[i] != 0 {
				kv.migratingShard[i] = true
			}
		}

		if oldShards[i] != kv.gid && newShards[i] == kv.gid {
			if oldShards[i] != 0 {
				kv.migratingShard[i] = true
			}
		}
	}
}

func (kv *ShardKV) isRequestDuplicate(clientId int64, requestId int, shardNum int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastRequestId, ifClientInRecord := kv.kvDB[shardNum].ClientRequestId[clientId]
	if !ifClientInRecord {
		return false
	}

	return requestId <= lastRequestId
}

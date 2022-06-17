package shardkv

import "time"

func (kv *ShardKV) PullNewConfigLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		lastConfigNum := kv.config.Num
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !isLeader {
			time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
			continue
		}

		newConfig := kv.mck.Query(lastConfigNum + 1)
		if newConfig.Num == lastConfigNum+1 {
			op := Op{
				Operation: NEWCONFIG,
				NewConfig: newConfig,
			}

			kv.mu.Lock()
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.rf.Start(op)
			}
			kv.mu.Unlock()
		}

		time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	configNum := kv.config.Num
	kv.mu.Unlock()

	if args.ConfigNum > configNum {
		reply.Err = ErrConfigNum
		reply.ConfigNum = configNum
		return
	}

	if args.ConfigNum < configNum {
		reply.Err = OK
		return
	}

	if kv.CheckMigrateState(args.MigrateData) {
		reply.Err = OK
		return
	}

	op := Op{
		Operation:        MIGRATE,
		MigrateData:      args.MigrateData,
		MigrateConfigNum: args.ConfigNum,
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
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		tempConfig := kv.config.Num
		kv.mu.Unlock()

		if args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) && isLeader {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		kv.mu.Lock()
		tempConfig := kv.config.Num
		kv.mu.Unlock()
		if raftCommitOp.MigrateConfigNum == args.ConfigNum && args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) {
			reply.ConfigNum = tempConfig
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

func (kv *ShardKV) SendShardToOtherGroupLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !isLeader {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		notMigrateing := true
		kv.mu.Lock()
		for shard := 0; shard < NShards; shard++ {
			if kv.migratingShard[shard] {
				notMigrateing = false
			}
		}
		kv.mu.Unlock()

		if notMigrateing {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		isNeedSend, sendData := kv.isDataSent()
		if !isNeedSend {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		kv.sendShardComponent(sendData)
		time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
	}
}

func (kv *ShardKV) sendShardComponent(sendData map[int][]ShardComponent) {
	for aimGid, ShardComponents := range sendData {
		kv.mu.Lock()
		args := &MigrateShardArgs{ConfigNum: kv.config.Num, MigrateData: make([]ShardComponent, 0)}
		groupServers := kv.config.Groups[aimGid]
		kv.mu.Unlock()

		for _, component := range ShardComponents {
			tempComponent := ShardComponent{ShardIndex: component.ShardIndex, KVDB: make(map[string]string), ClientRequestId: make(map[int64]int)}
			cloneShard(&tempComponent, component)
			args.MigrateData = append(args.MigrateData, tempComponent)
		}

		go kv.callMigrateRPC(groupServers, args)
	}
}

func (kv *ShardKV) isDataSent() (bool, map[int][]ShardComponent) {
	sendData := kv.MakeSendShardComponent()
	if len(sendData) == 0 {
		return false, make(map[int][]ShardComponent)
	}
	return true, sendData
}

func (kv *ShardKV) MakeSendShardComponent() map[int][]ShardComponent {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendData := make(map[int][]ShardComponent)

	for i := 0; i < NShards; i++ {
		newGid := kv.config.Shards[i]
		if kv.migratingShard[i] && kv.gid != newGid {
			tmpComponent := ShardComponent{
				ShardIndex:      i,
				KVDB:            make(map[string]string),
				ClientRequestId: make(map[int64]int),
			}

			cloneShard(&tmpComponent, kv.kvDB[i])
			sendData[newGid] = append(sendData[newGid], tmpComponent)
		}
	}
	return sendData
}

func (kv *ShardKV) callMigrateRPC(groupServers []string, args *MigrateShardArgs) {
	for _, groupMember := range groupServers {
		callEnd := kv.make_end(groupMember)
		migrateReply := MigrateShardReply{}
		ok := callEnd.Call("ShardKV.MigrateShard", args, &migrateReply)
		kv.mu.Lock()
		configNum := kv.config.Num
		kv.mu.Unlock()

		if ok && migrateReply.Err == OK {
			if configNum != args.ConfigNum || kv.CheckMigrateState(args.MigrateData) {
				return
			} else {
				kv.rf.Start(Op{Operation: MIGRATE, MigrateData: args.MigrateData, MigrateConfigNum: args.ConfigNum})
				return
			}
		}
	}
}

package shardctrler

import "6.824/raft"

func (sc *ShardCtrler) ReadRaftApplyCommandLoop() {
	for message := range sc.applyCh {
		if message.CommandValid {
			sc.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			sc.GetSnapShotFromRaft(message)
		}
	}
}

func (sc *ShardCtrler) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= sc.lastSSPointRaftLogIndex {
		return
	}

	if !sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
		if op.Operation == JoinOp {
			sc.ExecJoinOnController(op)
		}
		if op.Operation == LeaveOp {
			sc.ExecLeaveOnController(op)
		}
		if op.Operation == MoveOp {
			sc.ExecMoveOnController(op)
		}
	}

	if sc.maxraftstate != -1 {
		sc.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	sc.SendMessageToWaitChan(op, message.CommandIndex)
}

func (sc *ShardCtrler) SendMessageToWaitChan(op Op, raftIndex int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

func (sc *ShardCtrler) ExecQueryOnController(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	if op.Num_Query == -1 || op.Num_Query >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[op.Num_Query]
	}
}

func (sc *ShardCtrler) ExecJoinOnController(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	config := sc.MakeJoinConfig(op.Servers_Join)
	sc.configs = append(sc.configs, *config)
}

func (sc *ShardCtrler) ExecLeaveOnController(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	config := sc.MakeLeaveConfig(op.Gids_Leave)
	sc.configs = append(sc.configs, *config)
}

func (sc *ShardCtrler) ExecMoveOnController(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	config := sc.MakeMoveConfig(op.Shard_Move, op.Gid_Move)
	sc.configs = append(sc.configs, *config)
}

func (sc *ShardCtrler) MakeMoveConfig(shard int, gid int) *Config {
	config := sc.configs[len(sc.configs)-1]
	retConfig := Config{
		Num:    len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{},
	}

	for idx, gid := range config.Shards {
		retConfig.Shards[idx] = gid
	}

	retConfig.Shards[shard] = gid

	for gid, servers := range config.Groups {
		retConfig.Groups[gid] = servers
	}

	return &retConfig
}

func (sc *ShardCtrler) MakeLeaveConfig(gids []int) *Config {
	config := sc.configs[len(sc.configs)-1]
	tmpGroups := make(map[int][]string)
	isLeaved := make(map[int]bool)

	for _, gid := range gids {
		isLeaved[gid] = true
	}

	for gid, preServers := range config.Groups {
		tmpGroups[gid] = preServers
	}

	for _, gid := range gids {
		delete(tmpGroups, gid)
	}

	shards := config.Shards
	GidToShardNumMap := make(map[int]int)
	for gid := range tmpGroups {
		if !isLeaved[gid] {
			GidToShardNumMap[gid] = 0
		}
	}

	for idx, gid := range config.Shards {
		if gid != 0 {
			if isLeaved[gid] {
				shards[idx] = 0
			} else {
				GidToShardNumMap[gid]++
			}
		}
	}

	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tmpGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap, shards),
		Groups: tmpGroups,
	}
}

func (sc *ShardCtrler) MakeJoinConfig(servers map[int][]string) *Config {
	config := sc.configs[len(sc.configs)-1]
	tmpGroups := make(map[int][]string)

	for gid, preServers := range config.Groups {
		tmpGroups[gid] = preServers
	}

	for gid, newServers := range servers {
		tmpGroups[gid] = newServers
	}

	GidToShardNumMap := make(map[int]int)
	for gid := range tmpGroups {
		GidToShardNumMap[gid] = 0
	}
	for _, gid := range config.Shards {
		if gid != 0 {
			GidToShardNumMap[gid]++
		}
	}

	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tmpGroups,
		}
	} else {
		return &Config{
			Num:    len(sc.configs),
			Shards: sc.reBalanceShards(GidToShardNumMap, config.Shards),
			Groups: tmpGroups,
		}
	}
}

func (sc *ShardCtrler) ifRequestDuplicate(clientId int64, requestId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastRequestId, exit := sc.lastRequestId[clientId]
	if !exit {
		return false
	}

	return requestId <= lastRequestId
}

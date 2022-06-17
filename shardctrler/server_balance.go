package shardctrler

func (sc *ShardCtrler) BalanceLeave(config *Config, leaveGids []int) {
	length := len(config.Groups)
	if length == 0 {
		config.Shards = [10]int{}
		return
	}

	average := NShards / length
	plusNum := NShards - average*length
	avgNum := length - plusNum

	assignGid := 0
	for gid, _ := range config.Groups {
		if gid != 0 {
			assignGid = gid
		}
	}

	for shards, gid := range config.Shards {
		isLeaved := false

		for _, leaveGid := range leaveGids {
			if gid == leaveGid {
				isLeaved = true
			}
		}

		if gid == 0 || isLeaved {
			config.Shards[shards] = assignGid
		}
	}

	countArr := make(map[int][]int)

	for shard, gid := range config.Shards {
		if _, exit := countArr[gid]; exit {
			countArr[gid] = append(countArr[gid], shard)
		} else {
			countArr[gid] = make([]int, 0)
			countArr[gid] = append(countArr[gid], shard)
		}
	}

	for gid, _ := range config.Groups {
		if _, exist := countArr[gid]; !exist {
			countArr[gid] = make([]int, 0)
		}
	}

	for {
		if isBalance(average, avgNum, plusNum, countArr) {
			break
		}

		maxShardsNum := -1
		maxGid := -1
		minShardsNum := NShards * 10
		minGid := -1

		for gid, shards := range countArr {
			if len(shards) >= maxShardsNum {
				maxShardsNum = len(shards)
				maxGid = gid
			}

			if len(shards) <= minShardsNum {
				minShardsNum = len(shards)
				minGid = gid
			}
		}

		fromGid := maxGid
		movedShard := countArr[maxGid][maxShardsNum-1]
		toGid := minGid

		countArr[fromGid] = countArr[fromGid][:maxShardsNum-1]
		countArr[toGid] = append(countArr[toGid], movedShard)
		config.Shards[movedShard] = toGid
	}
}

func (sc *ShardCtrler) BalanceJoin(config *Config, joinSevers map[int][]string) {
	length := len(config.Groups)
	average := NShards / length
	plusNum := NShards - average*length
	avgNum := length - plusNum

	assignGid := 0
	for gid, _ := range config.Groups {
		if gid != 0 {
			assignGid = gid
		}
	}

	for shards, gid := range config.Shards {
		if gid == 0 {
			config.Shards[shards] = assignGid
		}
	}

	countArr := make(map[int][]int)

	for shard, gid := range config.Shards {
		if _, exit := countArr[gid]; exit {
			countArr[gid] = append(countArr[gid], shard)
		} else {
			countArr[gid] = make([]int, 0)
			countArr[gid] = append(countArr[gid], shard)
		}
	}

	if len(countArr) >= 10 {
		return
	}

	for gid, _ := range joinSevers {
		if _, exist := countArr[gid]; !exist {
			countArr[gid] = make([]int, 0)
		}
	}

	for {
		if isBalance(average, avgNum, plusNum, countArr) {
			break
		}

		maxShardsNum := -1
		maxGid := -1
		minShardsNum := NShards * 10
		minGid := -1

		for gid, shards := range countArr {
			if len(shards) >= maxShardsNum {
				maxShardsNum = len(shards)
				maxGid = gid
			}

			if len(shards) <= minShardsNum {
				minShardsNum = len(shards)
				minGid = gid
			}
		}

		fromGid := maxGid
		movedShard := countArr[maxGid][maxShardsNum-1]
		toGid := minGid

		countArr[fromGid] = countArr[fromGid][:maxShardsNum-1]
		countArr[toGid] = append(countArr[toGid], movedShard)
		config.Shards[movedShard] = toGid
	}
}

func (sc *ShardCtrler) reBalanceShards(GidToShardNumMap map[int]int, shards [NShards]int) [NShards]int {
	length := len(GidToShardNumMap)
	average := NShards / length
	plusNum := NShards % length
	sortNum := sortNumArray(GidToShardNumMap)

	for i := length - 1; i >= 0; i-- {
		resNum := average
		if !isAvg(length, plusNum, i) {
			resNum++
		}

		if resNum < GidToShardNumMap[sortNum[i]] {
			fromGid := sortNum[i]
			changeNum := GidToShardNumMap[fromGid] - resNum

			for idx, gid := range shards {
				if changeNum <= 0 {
					break
				}

				if gid == fromGid {
					shards[idx] = 0
					changeNum--
				}
			}
			GidToShardNumMap[fromGid] = resNum
		}
	}

	for i := 0; i < length; i++ {
		resNum := average
		if !isAvg(length, plusNum, i) {
			resNum++
		}

		if resNum > GidToShardNumMap[sortNum[i]] {
			toGid := sortNum[i]
			changeNum := resNum - GidToShardNumMap[toGid]

			for idx, gid := range shards {
				if changeNum <= 0 {
					break
				}

				if gid == 0 {
					changeNum--
					shards[idx] = toGid
				}
			}

			GidToShardNumMap[toGid] = resNum
		}
	}

	return shards
}

func isAvg(length int, plusNum int, idx int) bool {
	if idx < length-plusNum {
		return true
	} else {
		return false
	}
}

func sortNumArray(GidToShardNumMap map[int]int) []int {
	length := len(GidToShardNumMap)

	numArray := make([]int, 0, length)
	for gid, _ := range GidToShardNumMap {
		numArray = append(numArray, gid)
	}

	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GidToShardNumMap[numArray[j]] < GidToShardNumMap[numArray[j-1]] || (GidToShardNumMap[numArray[j]] == GidToShardNumMap[numArray[j-1]] && numArray[j] < numArray[j-1]) {
				numArray[j], numArray[j-1] = numArray[j-1], numArray[j]
			}
		}
	}
	return numArray
}

func isBalance(average int, avgNum int, plusNum int, countArray map[int][]int) bool {
	avgCnt := 0
	avgPlusCnt := 0
	for gid, shards := range countArray {
		if len(shards) == average && gid != 0 {
			avgCnt++
		}

		if len(shards) == average+1 && gid != 0 {
			avgPlusCnt++
		}
	}

	return avgCnt == avgNum && avgPlusCnt == plusNum
}

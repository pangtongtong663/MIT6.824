package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rule 1
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		return
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ConflictIndex = -1

	if rf.state != FOLLOWER {
		rf.changeState(TO_FOLLOWER, true)
	} else {
		rf.lastElectionTime = time.Now()
		rf.persist()
	}

	if rf.lastSSPointIndex > args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = rf.getLastIndex()
		return
	} else {
		if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.getLogTermWithIndex(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastSSPointIndex; index-- {
				if rf.getLogTermWithIndex(index) != tempTerm {
					reply.ConflictIndex = index + 1
					break
				}
			}
			return
		}
	}

	//rule 3 4
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastSSPointIndex], args.Entries...)
	rf.persist()

	//rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(FOLLOWER, args.LeaderCommit)
	}

}

func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1
			if prevLogIndex < rf.lastSSPointIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			aeArgs := AppendEntriesArgs{}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]Entry, 0)
				entries = append(entries, rf.log[rf.nextIndex[server]-rf.lastSSPointIndex:]...)
				prevIndex, preTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  preTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
			} else {
				prevIndex, preTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  preTerm,
					Entries:      []Entry{},
					LeaderCommit: rf.commitIndex,
				}
			}
			aeReply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server, &aeArgs, &aeReply)

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != LEADER {
					return
				}

				if aeReply.Term > rf.currentTerm {
					rf.currentTerm = aeReply.Term
					rf.changeState(TO_FOLLOWER, true)
					return
				}

				if aeReply.Success {
					rf.matchIndex[server] = aeArgs.PrevLogIndex + len(aeArgs.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex(LEADER, 0)
				} else {
					if aeReply.ConflictIndex != -1 {
						rf.nextIndex[server] = aeReply.ConflictIndex
					}
				}
			}
		}(index)
	}
}

func (rf *Raft) getLogWithIndex(globalIndex int) Entry {

	return rf.log[globalIndex-rf.lastSSPointIndex]
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	//log.Printf("[GetLogTermWithIndex] Sever %d,lastSSPindex %d ,len %d",rf.me,rf.lastSSPointIndex,len(rf.log))
	if globalIndex-rf.lastSSPointIndex == 0 {
		return rf.lastSSPointTerm
	}
	return rf.log[globalIndex-rf.lastSSPointIndex].Term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderAppendEntriesTicker() {
	for rf.killed() == false {
		time.Sleep(HEARTBEAT_TIMEOUT * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedToAppliedTicker() {
	// put the committed entry to apply on the state machine
	for rf.killed() == false {
		time.Sleep(APPLIED_TIMEOUT * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		// log.Printf("[!!!!!!--------!!!!!!!!-------]Restart, LastSSP: %d, LastApplied :%d, commitIndex %d",rf.lastSSPointIndex,rf.lastApplied,rf.commitIndex)
		//log.Printf("[ApplyEntry] LastApplied %d, commitIndex %d, lastSSPindex %d, len %d, lastIndex %d",rf.lastApplied,rf.commitIndex,rf.lastSSPointIndex, len(rf.log),rf.getLastIndex())
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			//for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			//DPrintf("[ApplyEntry---] %d apply entry index %d, command %v, term %d, lastSSPindex %d",rf.me,rf.lastApplied,rf.getLogWithIndex(rf.lastApplied).Command,rf.getLogWithIndex(rf.lastApplied).Term,rf.lastSSPointIndex)
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.getLogWithIndex(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyCh <- messages
		}
	}

}

func (rf *Raft) updateCommitIndex(role int, leaderCommit int) {

	if role != LEADER {
		if leaderCommit > rf.commitIndex {
			lastNewIndex := rf.getLastIndex()
			if leaderCommit >= lastNewIndex {
				rf.commitIndex = lastNewIndex
			} else {
				rf.commitIndex = leaderCommit
			}
		}
		DPrintf("[CommitIndex] Fllower %d commitIndex %d", rf.me, rf.commitIndex)
		return
	}

	if role == LEADER {
		rf.commitIndex = rf.lastSSPointIndex
		//for index := rf.commitIndex+1;index < len(rf.log);index++ {
		//for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
		for index := rf.getLastIndex(); index >= rf.lastSSPointIndex+1; index-- {
			sum := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					sum += 1
					continue
				}
				if rf.matchIndex[i] >= index {
					sum += 1
				}
			}

			//log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSSPointIndex, index, rf.commitIndex, rf.getLastIndex())
			if sum >= len(rf.peers)/2+1 && rf.getLogTermWithIndex(index) == rf.currentTerm {
				rf.commitIndex = index
				break
			}

		}
		DPrintf("[CommitIndex] Leader %d(term%d) commitIndex %d", rf.me, rf.currentTerm, rf.commitIndex)
		return
	}

}

package raft

import "time"

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastSSPointIndex >= index || index > rf.commitIndex {
		return
	}
	tempLog := make([]Entry, 0)
	tempLog = append(tempLog, Entry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.getLogWithIndex(i))
	}

	if index == rf.getLastIndex()+1 {
		rf.lastSSPointTerm = rf.getLastTerm()
	} else {
		rf.lastSSPointTerm = rf.getLogTermWithIndex(index)
	}

	rf.lastSSPointIndex = index

	rf.log = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	if rf.state != FOLLOWER {
		rf.changeState(TO_FOLLOWER, true)
	} else {
		rf.lastElectionTime = time.Now()
		rf.persist()
	}

	if rf.lastSSPointIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludeIndex
	tempLog := make([]Entry, 0)
	tempLog = append(tempLog, Entry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.getLogWithIndex(i))
	}

	rf.lastSSPointIndex = args.LastIncludeIndex
	rf.lastSSPointTerm = args.LastIncludeTerm
	rf.log = tempLog

	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastSSPointTerm,
		SnapshotIndex: rf.lastSSPointIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()

	ssArgs := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastSSPointIndex,
		rf.lastSSPointTerm,
		rf.persister.ReadSnapshot(),
	}

	ssReply := InstallSnapshotReply{}

	rf.mu.Unlock()

	re := rf.sendSnapShot(server, &ssArgs, &ssReply)

	if re {
		rf.mu.Lock()
		if rf.state != LEADER || rf.currentTerm != ssArgs.Term {
			rf.mu.Unlock()
			return
		}

		if ssReply.Term > rf.currentTerm {
			rf.changeState(FOLLOWER, true)
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = ssArgs.LastIncludeIndex
		rf.nextIndex[server] = ssArgs.LastIncludeIndex + 1

		rf.mu.Unlock()
	}

}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

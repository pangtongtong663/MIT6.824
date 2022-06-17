package raft

import "time"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rule 1
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//rule 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.VoteGranted = false
		rf.changeState(TO_FOLLOWER, false)
		rf.persist()
	}

	if rf.UpToDate(args.LastLogIndex, args.LastLogTerm) == false {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.lastElectionTime = time.Now()
		rf.persist()
		return
	}

}

func (rf *Raft) candidateJoinElection() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			rvArgs := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  rf.getLastTerm(),
				LastLogIndex: rf.getLastIndex(),
			}

			rvReply := RequestVoteReply{}
			rf.mu.Unlock()

			re := rf.sendRequestVote(server, &rvArgs, &rvReply)

			if re == true {
				rf.mu.Lock()
				if rf.state != CANDIDATE || rvArgs.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if rvReply.VoteGranted == true && rf.currentTerm == rvArgs.Term {
					rf.getVoteNums += 1
					if rf.getVoteNums >= len(rf.peers)/2+1 {
						rf.changeState(TO_LEADER, true)
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}

				if rvReply.Term > rvArgs.Term {
					if rf.currentTerm < rvReply.Term {
						rf.currentTerm = rvReply.Term
					}
					rf.changeState(TO_FOLLOWER, false)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				return
			}
		}(index)
	}
}

func (rf *Raft) candidateElectionTicker() {
	for rf.killed() == false {
		curTime := time.Now()
		randTime := getRand(int64(rf.me))
		time.Sleep(time.Duration(randTime) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastElectionTime.Before(curTime) && rf.state != LEADER {
			rf.changeState(TO_CANDIDATE, true)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) changeState(toChange int, resetTime bool) {
	if toChange == TO_FOLLOWER {
		rf.state = FOLLOWER
		rf.getVoteNums = 0
		rf.votedFor = -1
		rf.persist()
		if resetTime {
			rf.lastElectionTime = time.Now()
		}
	}

	if toChange == TO_CANDIDATE {
		rf.state = CANDIDATE
		rf.getVoteNums = 1
		rf.votedFor = rf.me
		rf.currentTerm += 1
		rf.persist()
		rf.candidateJoinElection()
		rf.lastElectionTime = time.Now()
	}

	if toChange == TO_LEADER {
		rf.state = TO_LEADER
		rf.votedFor = -1
		rf.getVoteNums = 0
		rf.persist()

		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastIndex() + 1
		}

		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = rf.getLastIndex()
		rf.lastElectionTime = time.Now()
	}
}

func (rf *Raft) UpToDate(index int, term int) bool {

	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

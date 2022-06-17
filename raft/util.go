package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1 + rf.lastSSPointIndex
}

func (rf *Raft) getLastTerm() int {
	if len(rf.log)-1 == 0 {
		return rf.lastSSPointTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	// TODO fix it in lab4
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex > lastIndex {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.getLogTermWithIndex(newEntryBeginIndex)
}

func getRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

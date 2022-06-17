package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
)

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)

	enc.Encode(rf.lastSSPointIndex)
	enc.Encode(rf.lastSSPointTerm)

	return w.Bytes()
}

func (rf *Raft) persist() {

	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var read_currentTerm int
	var read_voteFor int
	var read_log []Entry
	var read_lastSSPointIndex int
	var read_lastSSPointTerm int

	if dec.Decode(&read_currentTerm) != nil ||
		dec.Decode(&read_voteFor) != nil ||
		dec.Decode(&read_log) != nil ||
		dec.Decode(&read_lastSSPointIndex) != nil ||
		dec.Decode(&read_lastSSPointTerm) != nil {
		log.Printf("Server %d readPersisit exits problems!", rf.me)
	} else {
		rf.currentTerm = read_currentTerm
		rf.votedFor = read_voteFor
		rf.log = read_log
		rf.lastSSPointIndex = read_lastSSPointIndex
		rf.lastSSPointTerm = read_lastSSPointTerm
	}

}

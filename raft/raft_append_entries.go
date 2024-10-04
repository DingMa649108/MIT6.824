@@ -0,0 +1,297 @@
package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextLogTerm  int
	NextLogIndex int
}

func (rf *Raft) resetAppendEntriesTimersZero() {
	for _, timer := range rf.appendEntriesTimers {
		timer.Stop()
		timer.Reset(0)
	}
}

func (rf *Raft) resetAppendEntriesTimerZero(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(0)
}

func (rf *Raft) resetAppendEntriesTimer(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(HeartBeatInterval)
}

func (rf *Raft) isOutOfArgsAppendEntries(args *AppendEntriesArgs) bool {
	argsLastLogIndex := args.PrevLogIndex + len(args.Entries)
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if lastLogTerm == args.Term && argsLastLogIndex < lastLogIndex {
		return true
	}
	return false
}

func (rf *Raft) getStoreIndexByLogIndex(logIndex int) int {
	storeIndex := logIndex - rf.lastSnapshotIndex
	if storeIndex < 0 {
		return -1
	}
	return storeIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v receive a appendEntries: %+v", rf.me, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.changeRole(Role_Follower)
	rf.resetElectionTimer()

	_, lastLogIndex := rf.getLastLogTermAndIndex()
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.Success = false
		reply.NextLogIndex = rf.lastSnapshotIndex + 1
	} else if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.NextLogIndex = lastLogIndex + 1
	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		if rf.isOutOfArgsAppendEntries(args) {
			reply.Success = false
			reply.NextLogIndex = 0 
		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:1], args.Entries...)
			_, currentLogIndex := rf.getLastLogTermAndIndex()
			reply.NextLogIndex = currentLogIndex + 1
		}
	} else if args.PrevLogTerm == rf.logs[rf.getStoreIndexByLogIndex(args.PrevLogIndex)].Term {
		if rf.isOutOfArgsAppendEntries(args) {
			reply.Success = false
			reply.NextLogIndex = 0
		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:rf.getStoreIndexByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			_, currentLogIndex := rf.getLastLogTermAndIndex()
			reply.NextLogIndex = currentLogIndex + 1
		}
	} else {
		term := rf.logs[rf.getStoreIndexByLogIndex(args.PrevLogIndex)].Term
		index := args.PrevLogIndex
		for index > rf.commitIndex && index > rf.lastSnapshotIndex && rf.logs[rf.getStoreIndexByLogIndex(index)].Term == term {
			index--
		}
		reply.Success = false
		reply.NextLogIndex = index + 1
	}

	if reply.Success {
		DPrintf("%v current commit: %v, try to commit %v", rf.me, rf.commitIndex, args.LeaderCommit)
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.persist()
	DPrintf("%v role: %v, get appendentries finish,args = %v,reply = %+v", rf.me, rf.role, *args, *reply)
	rf.mu.Unlock()

}

func (rf *Raft) getAppendLogs(peerId int) (prevLogIndex int, prevLogTerm int, logEntries []LogEntry) {
	nextIndex := rf.nextIndex[peerId]
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		prevLogTerm = lastLogTerm
		prevLogIndex = lastLogIndex
		return
	}
	//logEntries = rf.logs[nextIndex-rf.lastSnapshotIndex:]
	logEntries = make([]LogEntry, lastLogIndex-nextIndex+1)
	copy(logEntries, rf.logs[nextIndex-rf.lastSnapshotIndex:])
	prevLogIndex = nextIndex - 1
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.logs[prevLogIndex-rf.lastSnapshotIndex].Term
	}

	return
}

func (rf *Raft) tryCommitLog() {
	_, lastLogIndex := rf.getLastLogTermAndIndex()
	hasCommit := false

	for i := rf.commitIndex + 1; i <= lastLogIndex; i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					DPrintf("%v role: %v,commit index %v", rf.me, rf.role, i)
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}

	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		for i := 0; i < 10 && !rf.killed(); i++ {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()

	select {
	case <-rpcTimer.C:
		DPrintf("%v role: %v, send append entries to peer %v TIME OUT!!!", rf.me, rf.role, server)
		return
	case <-ch:
		return
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peerId int) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.role != Role_Leader {
		rf.resetAppendEntriesTimer(peerId)
		rf.mu.Unlock()
		return
	}
	DPrintf("%v send append entries to peer %v", rf.me, peerId)

	prevLogIndex, prevLogTerm, logEntries := rf.getAppendLogs(peerId)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.resetAppendEntriesTimer(peerId)
	rf.mu.Unlock()

	rf.sendAppendEntries(peerId, &args, &reply)

	DPrintf("%v role: %v, send append entries to peer finish,%v,args = %+v,reply = %+v", rf.me, rf.role, peerId, args, reply)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.changeRole(Role_Follower)
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if rf.role != Role_Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		if reply.NextLogIndex > rf.nextIndex[peerId] {
			rf.nextIndex[peerId] = reply.NextLogIndex
			rf.matchIndex[peerId] = reply.NextLogIndex - 1
		}
		if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
			rf.tryCommitLog()
		}
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if reply.NextLogIndex != 0 {
		if reply.NextLogIndex > rf.lastSnapshotIndex {
			rf.nextIndex[peerId] = reply.NextLogIndex
			rf.resetAppendEntriesTimerZero(peerId)
		} else {
			go rf.sendInstallSnapshotToPeer(peerId)
		}
		rf.mu.Unlock()
		return
	} else {
		//reply.NextLogIndex = 0
	}

	rf.mu.Unlock()
	return

}
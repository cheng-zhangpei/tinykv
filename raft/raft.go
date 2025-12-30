// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	electionTick int
}

func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// 1. 初始化 RaftLog
	raftLog := newLog(c.Storage)
	// 2. 获取之前的 HardState (Term, Vote, Commit)
	// 因为节点可能是重启的，必须恢复之前的状态
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // 读取存储失败直接挂
	}
	// 3. 构造 Raft 结构体
	r := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower, // 启动默认 Follower
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTick:     c.ElectionTick, // 保存基准值
		// 初始化 elapsed 为 0
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
	// 4. 恢复 ConfState (节点列表)
	peers := c.peers
	if len(cs.Nodes) > 0 {
		peers = cs.Nodes
	}
	// 初始化 Prs
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1, Match: 0}
	}
	// 5. 应用 Config 中的 Applied 设置
	// (防止重复应用日志)
	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}
	// 6. 恢复 HardState 中的 Commit
	if hs.Commit > 0 {
		raftLog.committed = hs.Commit
	}

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// get the expectation of follower`s log state
	progress := r.Prs[to]
	//
	prevLogIndex := progress.Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	// corner case: when the follower lack too many entries that the leader`s entries was compacted
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			// send the snapshot to let follower get the entries from snapshot
			r.sendSnapshot(to)
			return false
		}
		return false
	}
	sendEntries := r.RaftLog.entries[progress.Next : r.RaftLog.LastIndex()+1]
	var mEntries []*pb.Entry
	// change the format to pointer
	for i := range sendEntries {
		mEntries = append(mEntries, &sendEntries[i])
	}

	// 4. 构造消息
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		// the lasted index and term of the follower
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		// the entries will be appended in the follower`s logEntries
		Entries: mEntries,
		Commit:  r.RaftLog.committed,
	}
	r.sendMsg(msg)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat() {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		Term:    r.Term,
	}
	r.broadcast(msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.tickHeartBeat()
		// StateCandidate也需要同时再次触发选举，不然可能一直卡在candidate状态，
		//因为有时候在偶数节点的情况可能出现平票的情况。这个时候无法选举leader
	case StateFollower, StateCandidate:
		r.tickElection()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None // 清空投票
	r.electionElapsed = 0
	// 随机化超时时间 ([min, 2*min])
	r.electionTimeout = r.electionTick + rand.Intn(r.electionTick)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true // 给自己投一票
	r.electionElapsed = 0
	r.electionTimeout = r.electionTick + rand.Intn(r.electionTick)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	for peer := range r.Prs {
		r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
		r.Prs[peer].Match = 0
	}

	ent := &pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: nil}
	r.RaftLog.append(ent)
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// if m.Term > r.Term no matter which state, the node should become the follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// ------------------------------------------tick------------------------------------------

func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0

		err := r.Step(pb.Message{
			From:    r.id,
			To:      r.id,
			MsgType: pb.MessageType_MsgBeat,
		})
		if err != nil {
			return
		}
	}
}

// Follower/Candidate
func (r *Raft) tickElection() {
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		err := r.Step(pb.Message{
			From:    r.id,
			To:      r.id,
			MsgType: pb.MessageType_MsgHup,
		})
		if err != nil {
			return
		}
	}
}

// ------------------------------------------step------------------------------------------

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		r.handleProposal(m)
	case pb.MessageType_MsgBeat:
		r.sendHeartbeat()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendLogEntryResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	}
}

// ------------------------------------------handler------------------------------------------

func (r *Raft) handleVoteRequest(m pb.Message) {

}

func (r *Raft) handleVoteResponse(m pb.Message) {

}

func (r *Raft) handleAppendLogEntryResponse(m pb.Message) {}

func (r *Raft) handlePropose(m pb.Message) {

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request(follower)
func (r *Raft) handleHeartbeat(m pb.Message) {
	// check the term and reject
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		r.sendMsg(msg)
		return
	}

	r.Lead = m.From
	r.electionElapsed = 0

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		// help the leader check the conflict
		Index: r.RaftLog.LastIndex(),
	}
	r.sendMsg(msg)
}
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	//When processing information, the compatibility of terms is always the primary consideration
	//we must address first.
	if m.Term < r.Term {
		return
	}
	// if the follower
	if m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}
func (r *Raft) handleBeat(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		Term:    r.Term,
	}
	r.broadcast(msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) handleProposal(m pb.Message) {

}

// -----------------------------------------tool----------------------------------------------
// campaign follower change to candidate
func (r *Raft) campaign() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	// 这里需要带上日志索引与任期
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)

	msg := pb.Message{
		From:    r.id,
		MsgType: pb.MessageType_MsgRequestVote,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastIndex,
	}
	r.broadcast(msg)
}

func (r *Raft) broadcast(m pb.Message) {
	for peerId, _ := range r.Prs {
		if peerId == r.id {
			continue
		}
		m.To = peerId
		msg := m
		r.sendMsg(msg)
	}
}

// bcastAppend Everyone needs different logs! So we need to rewrite the broadcast function.
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}
func (r *Raft) sendMsg(msg pb.Message) {
	if msg.Term == 0 {
		msg.Term = r.Term
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) appendEntries(entries []pb.Message) {

}
func (r *Raft) sendSnapshot(to uint64) {}

//-----------------------------------------

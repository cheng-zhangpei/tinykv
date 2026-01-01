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
	_ "log"
	"math/rand"
	"sort"
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
	// 初始化 Prs,注意这里初始化要考虑本身可能是有持久化部分的日志的！
	lastIndex := r.RaftLog.LastIndex() // 获取当前的 LastIndex
	for _, p := range peers {
		// 默认认为 Follower 需要最新的下一条
		r.Prs[p] = &Progress{Next: lastIndex + 1, Match: 0}
	}
	// 5. 应用 Config 中的 Applied 设置
	// (防止重复应用日志)
	var firstIndex uint64 = 0
	if len(r.RaftLog.entries) != 0 {
		firstIndex = r.RaftLog.entries[0].Index
	}

	if c.Applied > 0 {
		raftLog.applied = c.Applied
	} else if firstIndex > 1 {
		// 既然 1...firstIndex-1 都被 Compact 了，说明肯定 Apply 过了
		raftLog.applied = firstIndex - 1
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
	ents, err := r.RaftLog.Entries(progress.Next, r.RaftLog.LastIndex()+1)
	if err != nil {
		// 如果这里报错，通常是 panic 或者忽略
		return false
	}

	var mEntries []*pb.Entry
	for i := range ents {
		mEntries = append(mEntries, &ents[i])
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
	return true
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
	r.votes[r.id] = true
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

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
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
		r.handlePropose(m)
	case pb.MessageType_MsgBeat:
		r.sendHeartbeat()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendLogEntryResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
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
// handleVoteRequest follower and candidate handle vote request
func (r *Raft) handleVoteRequest(m pb.Message) {
	// 0. Term 检查 (Safety)
	if r.State == StateLeader {
		r.sendVoteResponse(m.From, true)
		return
	}
	if m.Term < r.Term {
		r.sendVoteResponse(m.From, true)
		return
	}
	canVote := (r.Vote == None) || (r.Vote == m.From)

	// 2.  Safety - Log Matching
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	isLogUpToDate := false
	if m.LogTerm > lastTerm {
		isLogUpToDate = true
	} else if m.LogTerm == lastTerm && m.Index >= lastIndex {
		isLogUpToDate = true
	}
	// 3. vote response
	if canVote && isLogUpToDate {
		r.Vote = m.From
		r.electionElapsed = 0
		r.sendVoteResponse(m.From, false)
	} else {
		r.sendVoteResponse(m.From, true)
	}
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if r.State != StateCandidate {
		return
	}
	r.votes[m.From] = !m.Reject
	granted := 0
	rejected := 0
	for _, support := range r.votes {
		if support {
			granted++
		} else {
			rejected++
		}
	}

	quorum := len(r.Prs)/2 + 1
	if granted >= quorum {
		r.becomeLeader()
		r.bcastAppend()
	} else if rejected >= quorum {
		// 如果半数以上的人都拒绝我了，这届我就别选了，变 Follower
		r.becomeFollower(r.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request(follower and candidate)
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 1. Term check
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, 0, 0) // 拒绝
		return
	}
	r.becomeFollower(m.Term, m.From)
	// prevLogIndex and prevLogTerm is the record from leader`s Progress of this follower
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm

	term, err := r.RaftLog.Term(prevLogIndex)
	// conflict occur, the record of the leader is not right
	if err != nil || term != prevLogTerm {
		r.sendAppendResponse(m.From, true, 0, r.RaftLog.LastIndex()+1) // Hint Index
		return
	}

	lastNewIndex := m.Index + uint64(len(m.Entries))
	// This appends operation includes truncation functionality,
	// as the current index may be deeper than the follower's latest index.
	// 因为一些重放的因素我们这里需要进行截断，不能将entries的比committed指针更小的部分放到append函数里面
	var entriesToAppend []*pb.Entry
	for i, ent := range m.Entries {
		// 如果这条日志的 Index 已经被 Commit 了，那肯定不能改，直接跳过
		if ent.Index <= r.RaftLog.committed {
			continue
		}
		// 如果这条日志已经存在，且 Term 一样，那也不用改 (幂等性)
		// 只有当 Term 不一样（冲突），或者 Index 超出了我的 LastIndex（新数据）时，才需要 append
		if ent.Index <= r.RaftLog.LastIndex() {
			existingTerm, _ := r.RaftLog.Term(ent.Index)
			if existingTerm == ent.Term {
				continue
			}
		}
		// 找到了第一个不一样的（冲突点）或者新数据点
		// 从这里开始，后面的所有都要 append（覆盖）
		for j := i; j < len(m.Entries); j++ {
			entriesToAppend = append(entriesToAppend, m.Entries[j])
		}
		break
	}
	// 调用 append (只会追加真正需要追加的部分)
	if len(entriesToAppend) > 0 {
		r.RaftLog.append(entriesToAppend...)
	}
	// 5. update Commit Index
	if m.Commit > r.RaftLog.committed {
		// Why take the minimum? Because even though the Leader has committed,
		// I haven't received that many logs yet, so I can only commit what I have.
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}
	r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex(), 0)
}
func (r *Raft) handleAppendLogEntryResponse(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	if m.Reject {
		//log.Printf("Reject: from=%d, hint=%d, newNext=%d", m.From, m.Index, r.Prs[m.From].Next)
		if m.Index > 0 {
			// r.Prs[m.From].Next = m.Index
			// 防止 Hint 瞎指，导致 Next 不降反升
			r.Prs[m.From].Next = min(m.Index, r.Prs[m.From].Next-1)
		} else {
			r.Prs[m.From].Next--
		}
		// Next would be zero,I do not know why....
		if r.Prs[m.From].Next < 1 {
			r.Prs[m.From].Next = 1
		}
		r.sendAppend(m.From)
	}

	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		if r.maybeCommit() {
			r.bcastAppend()
		}
	}
}

// handlePropose trigger the AppendLogEntries
func (r *Raft) handlePropose(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	// todo We pick out the confChange message,we should handle it separately
	lastIndex := r.RaftLog.LastIndex()
	for i, ent := range m.Entries {
		ent.Term = r.Term
		ent.Index = lastIndex + uint64(i) + 1
		if ent.EntryType == pb.EntryType_EntryConfChange {
			// 如果已有 PendingConfIndex，拒绝新的
			if r.PendingConfIndex > r.RaftLog.applied {
				// 拒绝: r.msgs = append(r.msgs, pb.Message{To: m.From, Type: pb.MessageType_MsgPropose, Reject: true})
				// 不过简单起见，2B 先不管 ConfChange
			}
			r.PendingConfIndex = ent.Index
		}
	}
	// update leader`s state
	r.RaftLog.append(m.Entries...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	// notify the all cluster
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
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
	// update commit pointer (actually we update the pointer in AppendEntry())
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
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
		Commit:  r.RaftLog.committed,
	}
	r.broadcast(msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {

	// Your Code Here (2C).
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

func (r *Raft) sendSnapshot(to uint64) {}
func (r *Raft) sendVoteResponse(to uint64, reject bool) {
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		// Double-check both the follower and the candidate.
		LogTerm: lastLogTerm,
		Index:   lastIndex,
		Reject:  reject,
	}
	r.sendMsg(msg)
}
func (r *Raft) sendAppendResponse(to uint64, reject bool, index, rejectHint uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}

	if reject {
		msg.Index = rejectHint
	} else {
		msg.Index = index
	}
	r.sendMsg(msg)
}

// maybeCommit trying to push commit pointer
// the pointer should point at the pos which most of the node committed
func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.Prs))
	for _, peer := range r.Prs {
		matches = append(matches, peer.Match)
		//log.Printf("Peer %d match: %d", id, peer.Match)
	}
	// sort the matches arr => to find the majority
	//log.Printf("Sorted matches: %v, N=%d, Committed=%d", matches, len(r.Prs), r.RaftLog.committed)
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j]
	})
	n := matches[len(matches)/2]
	if r.RaftLog.committed < n {
		// can update the pointer
		term, err := r.RaftLog.Term(n)
		// only can update committed pointer in current term
		if err == nil && term == r.Term {
			r.RaftLog.committed = n
			// successfully set the committed pointer
			return true
		}
	}
	//log.Printf("leader commit update %d", r.RaftLog.committed)
	return false
}

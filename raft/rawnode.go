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
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// 记录上一次 Ready 时的状态，用于比较是否有变化
	prevSoftState *SoftState
	prevHardState pb.HardState
}

func NewRawNode(config *Config) (*RawNode, error) {
	r := newRaft(config)
	return &RawNode{
		Raft: r,
		prevSoftState: &SoftState{
			Lead:      r.Lead,
			RaftState: r.State,
		},
		prevHardState: pb.HardState{
			Term:   r.Term,
			Vote:   r.Vote,
			Commit: r.RaftLog.committed,
		},
	}, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
// Ready is the only interface disclose to outside
func (rn *RawNode) Ready() Ready {
	// load the message in the raft(brain) to ready
	r := rn.Raft
	ready := Ready{
		Entries:          r.RaftLog.unstableEntries(), // the entries that did not be persisted
		CommittedEntries: r.RaftLog.nextEnts(),        // this LogEntry has been persisted and committed
		Messages:         r.msgs,
	}
	// soft state change
	if r.Lead != rn.prevSoftState.Lead || r.State != rn.prevSoftState.RaftState {
		ready.SoftState = &SoftState{
			r.Lead,
			r.State,
		}
	}
	// if HardState change?
	if rn.prevHardState.Term != r.Term || rn.prevHardState.Vote != r.Vote || rn.prevHardState.Commit != r.RaftLog.committed {
		ready.HardState = pb.HardState{
			Term:   r.Term,
			Vote:   r.Vote,
			Commit: r.RaftLog.committed,
		}
	}
	// snapshot check
	if r.RaftLog.pendingSnapshot != nil {
		ready.Snapshot = *r.RaftLog.pendingSnapshot
	}
	return ready
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	r := rn.Raft
	// 1. if SoftState change?
	if r.Lead != rn.prevSoftState.Lead || r.State != rn.prevSoftState.RaftState {
		return true
	}
	// 2. if HardState change?
	if rn.prevHardState.Term != r.Term || rn.prevHardState.Vote != r.Vote || rn.prevHardState.Commit != r.RaftLog.committed {
		return true
	}
	// 3. the entry buffer have the entry need to be persisted?
	if len(r.RaftLog.unstableEntries()) > 0 {
		return true
	}
	// 4. the entries committed but not applied to the state machine
	if len(r.RaftLog.nextEnts()) > 0 {
		return true
	}
	// 5. msg did not send out
	if len(r.msgs) < 0 {
		return true
	}
	// 6. did the raftLog have the pending snapshot
	if r.RaftLog.pendingSnapshot != nil {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// update soft and hard state
	if rd.SoftState != nil {
		rn.prevSoftState = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardState = rd.HardState
	}

	// update stable pointer
	if len(rd.Entries) > 0 {
		lastEnt := rd.Entries[len(rd.Entries)-1]
		rn.Raft.RaftLog.stableTo(lastEnt.Index, lastEnt.Term)
	}
	// update committed pointer
	if len(rd.CommittedEntries) > 0 {
		lastEnt := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		rn.Raft.RaftLog.appliedTo(lastEnt.Index)
	}

	if !IsEmptySnapshot(&rd.Snapshot) {
		rn.Raft.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
	// clear all message
	rn.Raft.msgs = nil
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

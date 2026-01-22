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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64
	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// 1. 获取 Storage 状态
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()

	// 2. 加载 entries
	// 注意：firstIndex 可能比 lastIndex 大（比如 Storage 为空），这时候 entries 应该是 nil
	// Storage.Entries(first, last+1)
	log.Infof("start recover the log, trying to move [%d,%d] in the store ->  log in memory", firstIndex, lastIndex)
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	// 3. 初始化 RaftLog
	l := &RaftLog{
		storage:         storage,
		committed:       firstIndex - 1,
		applied:         firstIndex - 1,
		stabled:         lastIndex, // 刚从 storage 读出来，肯定都是 stable 的
		entries:         entries,
		pendingSnapshot: nil,
	}

	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory,
func (l *RaftLog) maybeCompact() {

}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries
}

// unstableEntries return all the unstable entries
// return all entries before stabled pointer
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return make([]pb.Entry, 0) // return empty slice, not nil
	}

	firstIndex := l.entries[0].Index
	if l.stabled < firstIndex {
		return l.entries
	}

	offset := l.stabled - firstIndex + 1
	if offset >= uint64(len(l.entries)) {
		return make([]pb.Entry, 0)
	}

	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
// return (applied, committed] slice
// the committed pointer always be ahead of the applied pointer
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// 边界检查
	if len(l.entries) == 0 {
		return make([]pb.Entry, 0)
	}

	firstIndex := l.entries[0].Index

	// applied 的下一条是起点
	offset := l.applied + 1 - firstIndex
	// committed 的下一条是终点（左闭右开）
	endOffset := l.committed + 1 - firstIndex

	// 范围有效性检查
	if offset >= endOffset {
		return nil
	}

	// 切片越界检查
	if offset < 0 {
		offset = 0
	}
	if endOffset > uint64(len(l.entries)) {
		// 这种情况理论上不应该发生，意味着 committed 指向了 entries 之后的位置
		// 这里做个截断保护
		endOffset = uint64(len(l.entries))
	}

	return l.entries[offset:endOffset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	var index uint64
	// 1. we should confirm there are entries in the buffer
	if len(l.entries) > 0 {
		index = l.entries[len(l.entries)-1].Index
	}

	// 2. you should compare the index in buffer and snapshot
	if l.pendingSnapshot != nil {
		snapIndex := l.pendingSnapshot.Metadata.Index
		if snapIndex > index {
			index = snapIndex
		}
	}

	// ask for storage (when the node is just start and there are no data in the buffer and snapshot)
	if index == 0 {
		i, _ := l.storage.LastIndex()
		if i > index {
			index = i
		}
	}

	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}
	if len(l.entries) > 0 {
		firstIndex := l.entries[0].Index
		if i >= firstIndex {
			offset := i - firstIndex
			if offset < uint64(len(l.entries)) {
				return l.entries[offset].Term, nil
			}
		}
	}

	// 2. if the index locate in pendingSnapshot
	if l.pendingSnapshot != nil {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
	}

	// 3. check the index in the storage
	term, err := l.storage.Term(i)
	return term, err
}

// append append entries into the store
func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	after := ents[0].Index

	// Safety Check: can not change committed logEntries
	if after <= l.committed {
		panic("out of bound")
	}

	// 计算要追加的第一条日志在 entries 中的相对位置
	// l.entries[0].Index 是日志的第一条 Index (firstIndex)
	// 比如 firstIndex=100, after=102, 那么 offset = 102 - 100 = 2
	if len(l.entries) > 0 {
		firstIndex := l.entries[0].Index
		//  Truncate logic:
		// 如果 after 刚好接在最后一条后面，直接 append
		// 如果 after 在中间，需要截断
		if after > firstIndex {
			offset := after - firstIndex
			// 只保留 entries[:offset] 部分
			// 比如 entries=[100, 101, 102], after=101 (offset=1)
			// 保留 entries[:1] 即 [100]，扔掉 101, 102
			if offset < uint64(len(l.entries)) {
				l.entries = l.entries[:offset]
				// 关键点：如果你截断了日志，stabled 指针可能也失效了
				// 如果 stabled 指向了被截断的部分（比如 stabled=102），需要回退
				if l.stabled >= after {
					l.stabled = after - 1
				}
			}
		} else {
			// 如果 after <= firstIndex，说明新的日志覆盖了整个 entries
			// 直接清空重写（这种通常发生在 snapshot 后的全量同步）
			l.entries = nil
			l.stabled = after - 1
		}
	}

	// 执行真正的追加
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}

	return l.LastIndex()
}

// appliedTo advances the applied index to the given index.
// 推进 applied 指针：告诉 RaftLog，直到 index 为止的日志已经被状态机执行了。
func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	// 只有当 i 大于当前的 applied 且 小于等于 committed 时才更新
	// (applied 不能超过 committed，也没必要回退)
	if l.applied < i && i <= l.committed {
		l.applied = i
	}
}

// stableTo update stabled pointer
func (l *RaftLog) stableTo(i uint64, t uint64) {
	term, err := l.Term(i)
	if err != nil {
		return
	}
	// 只有 term 匹配才更新，防止过期的持久化通知
	if term == t && i > l.stabled {
		l.stabled = i
	}
}

// stableSnapTo 更新快照状态
func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
		// 快照存盘了，意味着 stabled 也应该推进到快照的位置
		if i > l.stabled {
			l.stabled = i
		}
	}
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry, error) {
	// 1. 范围无效检查
	if lo >= hi {
		return nil, nil
	}

	// 2. 检查 entries 是否为空
	if len(l.entries) == 0 {
		return nil, nil
	}

	firstIndex := l.entries[0].Index

	// 3. 检查 lo 是否已经被 Compact
	if lo < firstIndex {
		return nil, ErrCompacted
	}

	// 4. 检查 hi 是否越界
	if hi > l.LastIndex()+1 {
		// 理论上调用者应该保证不越界，但防御性编程 panic 一下也可以
		// panic(fmt.Sprintf("entries hi(%d) out of bound lastindex(%d)", hi, l.LastIndex()))
		// 或者这里简单截断到末尾
		hi = l.LastIndex() + 1
	}
	// 5. 计算相对下标并切片
	offset := lo - firstIndex
	return l.entries[offset : offset+hi-lo], nil
}

// IsEmptySnapshot if the snapshot is an empty snapshot
func IsEmptySnapshot(sp *pb.Snapshot) bool {
	return sp == nil || sp.Metadata == nil || sp.Metadata.Index == 0
}

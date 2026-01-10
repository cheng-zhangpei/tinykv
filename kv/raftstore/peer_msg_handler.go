package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if !d.RaftGroup.HasReady() {
		return
	}
	// proposal ready of the rawNode
	rd := d.RaftGroup.Ready()
	applySnapResult, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		return
	}
	// 这个Send是一个广播函数，这个广播针对在peer这个region中的节点之间互联，leader会将消息广播给所有的follower
	d.Send(d.ctx.trans, rd.Messages)
	if applySnapResult != nil {
		// update store meta ...
		ps := d.peerStorage
		// 将最新的 Region 信息注册到全局 StoreMeta
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.setRegion(ps.region, d.peer)
		d.ctx.storeMeta.Unlock()
		// 可能还需要处理 Region Split/Merge 的后遗症，但在 2C 里通常这就够了
	}
	// 6. 应用已提交日志 (Apply CommittedEntries) - 2B 核心业务逻辑
	// 这里才是解析 Header，执行 Put/Delete/Admin 的地方！
	kvWB := new(engine_util.WriteBatch) // 在循环外创建
	for _, entry := range rd.CommittedEntries {
		d.processCommittedEntry(&entry, kvWB) // 只操作内存 WB
		if d.stopped {
			// 既然已经 stopped (destroy) 了，说明刚才那个 entry 是自杀命令。
			// 那么 kvWB 里的任何更新（包括 ApplyState）都不应该再写入了！
			// 因为 Peer 已经把自己删干净了，这里再写就是“诈尸”。
			return
		}
		if d.peerStorage.AppliedIndex() < entry.Index {
			// 更新内存里的 AppliedIndex
			d.peerStorage.SetAppliedIndex(entry.Index)
			// 把 ApplyState 的持久化也放进 WB
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		}
	}
	// 循环结束后，一次性写盘
	if kvWB.Len() > 0 {
		kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	}
	d.RaftGroup.Advance(rd)

}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	for _, r := range req.Requests {
		var key []byte
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = r.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = r.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = r.Delete.Key
		}
		if len(key) > 0 {
			err := util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// 1. 预检查 (Leader? Term? StoreID?)

	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// 2. 【关键新增】拦截特殊 Admin Request
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			// 直接执行转让，不走日志
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			// 立即回复成功
			cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			})
			return // 结束，不往下走了

		case raft_cmdpb.AdminCmdType_ChangePeer:
			// 转换为 ConfChange 日志
			changePeer := msg.AdminRequest.ChangePeer
			cc := eraftpb.ConfChange{
				ChangeType: changePeer.ChangeType,
				NodeId:     changePeer.Peer.Id,
				Context:    nil,
			}
			// 序列化 Context
			if changePeer.Peer != nil {
				data, err := msg.Marshal() // 把整个 RaftCmdRequest 序列化进去
				if err != nil {
					cb.Done(ErrResp(err))
					return
				}
				cc.Context = data
			}

			// 发起 ProposeConfChange
			if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
				cb.Done(ErrResp(err))
				return
			}

			// 绑定 Callback (ConfChange 也是一条日志，有 Index)
			// 注意：ProposeConfChange 内部已经追加了日志，所以 LastIndex 已经是最新的了
			index := d.RaftGroup.Raft.RaftLog.LastIndex()
			term := d.RaftGroup.Raft.Term

			if cb != nil {
				d.proposals = append(d.proposals, &proposal{
					index: index,
					term:  term,
					cb:    cb,
				})
			}
			return // 结束，ConfChange 已经处理完了
		}
	}

	// 3. 普通请求 (Put/Delete/Compact/Split) 继续往下走
	data, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// 绑定回调
	index := d.RaftGroup.Raft.RaftLog.LastIndex() + 1
	term := d.RaftGroup.Raft.Term

	if cb != nil {
		d.proposals = append(d.proposals, &proposal{
			index: index,
			term:  term,
			cb:    cb,
		})
	}

	if err := d.RaftGroup.Propose(data); err != nil {
		if cb != nil {
			// 回滚队列
			d.proposals = d.proposals[:len(d.proposals)-1]
			cb.Done(ErrResp(err))
		}
		return
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	// 这里触发了快照
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
func (d *peerMsgHandler) processCommittedEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Errorf("%v failed to unmarshal conf change: %v", d.Tag, err)
			return
		}
		// 调用处理函数 (修改 Region 元数据，回调 Raft)
		d.processConfChange(entry, cc, kvWB)
		return
	}

	if len(entry.Data) == 0 {
		// 传一个 nil 的 resp，表示没有实际操作，handleCallback 会只负责清理过期 proposal
		d.handleCallback(entry.Index, entry.Term, nil)
		return
	}

	msg := &raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(entry.Data); err != nil {
		return
	}

	if len(entry.Data) == 0 {
		return
	}

	// 1. 【统一检查】RegionEpoch
	// 无论是普通请求还是 Admin 请求，首先检查版本号
	// 如果不对，直接回调错误，后续所有 Put/Get/Snap 统统不执行
	if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
		d.handleCallback(entry.Index, entry.Term, ErrResp(err))
		return
	}
	var resp *raft_cmdpb.RaftCmdResponse

	if msg.AdminRequest != nil {
		resp = d.processAdminRequest(msg.AdminRequest, kvWB)
	} else if len(msg.Requests) > 0 {
		resp = d.processRequests(msg, kvWB)
	}

	// 3. 【统一回复】处理 Callback
	d.handleCallback(entry.Index, entry.Term, resp)
}

// processRequests 纯粹负责执行 KV 操作和构造 Response
// 它不负责 Epoch 检查，也不负责调用 Callback
func (d *peerMsgHandler) processRequests(msg *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: make([]*raft_cmdpb.Response, 0, len(msg.Requests)),
	}

	for _, req := range msg.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			})

		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})

		case raft_cmdpb.CmdType_Get:
			// Get 不需要写 WriteBatch，直接读
			val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     &raft_cmdpb.GetResponse{Value: val},
			})

		case raft_cmdpb.CmdType_Snap:
			// Epoch 检查已经在 processCommittedEntry 做过了
			// 这里只需要返回 Region 信息
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
			})
		}
	}
	return resp
}

// 新增：处理 ConfChange 的专用函数
func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch) {
	// 1. 调用 RaftGroup 的 ApplyConfChange (通知 Raft 内核)
	// 这会更新 Raft 内存里的 Prs，并重置 PendingConfIndex
	_ = d.RaftGroup.ApplyConfChange(*cc)
	// 2. 解析 Context 获取 Peer 信息
	var msg raft_cmdpb.RaftCmdRequest
	if err := msg.Unmarshal(cc.Context); err != nil {
		// 如果 Context 为空或解析失败，说明可能只是为了测试或者某种特殊情况
		// 只要不影响 Raft 核心逻辑就行
	}
	// 3. 修改 Region 元数据
	region := d.Region()
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if util.FindPeer(region, cc.NodeId) == nil {
			// 从 Context 里拿到完整的 Peer 信息 (StoreId 等)
			// 注意：这里需要确保 Propose 的时候把 ChangePeerRequest 塞进了 cc.Context
			req := msg.AdminRequest.ChangePeer
			region.Peers = append(region.Peers, req.Peer)
			log.Infof("%v add peer %v", d.Tag, req.Peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		// 如果删的是自己，准备自杀
		if cc.NodeId == d.PeerId() {
			d.destroyPeer()
			return // 自杀了就不用往下执行了
		}
		// 从 Peers 列表里移除
		if util.FindPeer(region, cc.NodeId) != nil {
			util.RemovePeer(region, cc.NodeId)
			log.Infof("%v remove peer %v", d.Tag, cc.NodeId)
		}
	}
	// 4. 更新 ConfVer记录版本号并持久化
	region.RegionEpoch.ConfVer++
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)

	// 5. 更新 StoreMeta 里的 Region 缓存,这里就是globalContext中的值
	// 因为 d.Region() 返回的是缓存的引用，上面修改 region 其实已经改了缓存
	// 但为了线程安全，最好加锁或者重新 Set 一下
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regions[d.regionId] = region
	d.ctx.storeMeta.Unlock()

	// 6. 别忘了通知 Callback！(虽然 AdminRequest.ChangePeer 是空的 Response)
	// ProposeConfChange 的时候也挂了 Callback
	d.handleCallback(entry.Index, entry.Term, &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: region},
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		},
	})
}

// processAdminRequest 处理 Split/Compact
// 注意：TransferLeader 和 ChangePeer 不会走到这里！
func (d *peerMsgHandler) processAdminRequest(req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	reqResp := &raft_cmdpb.AdminResponse{
		CmdType: req.CmdType,
	}

	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactLog := req.CompactLog
		if compactLog.CompactIndex >= d.peerStorage.truncatedIndex() {
			d.ScheduleCompactLog(compactLog.CompactIndex)
			d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm

			// 持久化 ApplyState
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		}
		reqResp.CompactLog = &raft_cmdpb.CompactLogResponse{}

	case raft_cmdpb.AdminCmdType_Split:
		split := req.Split
		// 检查一下worker生成的 splitKey是否还在
		if err := util.CheckKeyInRegion(split.SplitKey, d.Region()); err != nil {
			return ErrResp(err)
		}
		d.onRegionSplit(split, kvWB)
		// 这里我有个疑问，这个findSiblingRegion是如何将这个region分裂出来的内容拿出来的
		reqResp.Split = &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{d.Region(), d.findSiblingRegion()},
			// findSiblingRegion 是那个新分裂出来的 Region，
			// 但 onRegionSplit 还没执行完可能拿不到？
			// 或者简单点，onRegionSplit 返回新 Region，或者这里先留空，Client 会自己重试
		}
	}

	return &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: reqResp,
	}
}

func (d *peerMsgHandler) handleCallback(index uint64, term uint64, resp *raft_cmdpb.RaftCmdResponse) {
	// 循环处理 proposals，直到队列为空或者找到匹配的 proposal
	for len(d.proposals) > 0 {
		p := d.proposals[0]

		// 1. 如果 proposal 的 term 比当前 entry 的 term 还要小
		// 说明这是旧 Term 留下的 proposal，已经没用了（被新的日志覆盖了，或者 Leader 换了）
		// 应该通知 Client "Stale Command" 或者直接报错
		if p.term < term {
			NotifyStaleReq(p.term, p.cb) // 辅助函数通知错误
			d.proposals = d.proposals[1:]
			continue
		}

		// 2. 如果 proposal 的 index 比当前 entry 的 index 小
		// 说明这个 proposal 对应的日志可能被丢弃了，或者跳过了
		if p.index < index {
			NotifyStaleReq(p.term, p.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		// 3. 如果 index 还没到，说明还没轮到它（未来才会 commit）
		// 此时应该退出循环，等待下一次 handleCallback
		if p.index > index {
			break
		}

		// 4. 找到了！p.index == index && p.term == term
		// 这是正常匹配的情况

		// 【特殊处理 Snap】
		// 检查 resp 是否匹配 Snap 请求
		if resp == nil {
			NotifyStaleReq(p.term, p.cb) // 或者返回一个特定的 Err
			d.proposals = d.proposals[1:]
			continue
		}

		// 正常的 Response 处理 (Snap Txn 挂载等)
		if len(resp.Responses) > 0 && resp.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
			if p.cb.Txn == nil {
				p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
		}

		p.cb.Done(resp)
		d.proposals = d.proposals[1:]
		return
	}
}

func NotifyStaleReq(term uint64, cb *message.Callback) {
	cb.Done(ErrResp(&util.ErrStaleCommand{}))
}
func (d *peerMsgHandler) ErrResp(err error) *raft_cmdpb.RaftCmdResponse {
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{
			// 这里会自动把 error 转换成 raft_cmdpb.Error 结构
			Error: util.RaftstoreErrToPbError(err),
		},
	}
	return resp
}

// onRegionSplit 这个函数是用于处理Region分裂的主要的函数
func (d *peerMsgHandler) onRegionSplit(split *raft_cmdpb.SplitRequest, kvWB *engine_util.WriteBatch) {
	// 先复制一些参数到新的region里面
	newRegion := &metapb.Region{
		Id:       split.NewRegionId,
		StartKey: split.SplitKey,
		EndKey:   d.Region().EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: d.Region().RegionEpoch.ConfVer, // 继承 ConfVer
			Version: d.Region().RegionEpoch.Version, // 继承 Version (稍后统一 +1)
		},
		Peers: make([]*metapb.Peer, 0),
	}
	// 将自己的peer复制一下
	for i, peer := range d.Region().Peers {
		newRegion.Peers = append(newRegion.Peers, &metapb.Peer{
			Id:      split.NewPeerIds[i],
			StoreId: peer.StoreId,
		})
	}
	// 2. 修改老 Region (Left Part)
	d.Region().EndKey = split.SplitKey
	d.Region().RegionEpoch.Version++ // 老 Region 版本号 +1
	newRegion.RegionEpoch.Version++  // 新 Region 版本号 +1 (和老的一样)
	// 3. 持久化 (Meta 信息)
	// 同时保存两个 Region 的状态，保证原子性
	meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	// 4、更新路由表
	d.ctx.storeMeta.Lock()
	// 需要将在B树索引里面的内容给删了，否则B树的构建可能会出问题
	d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: d.Region()})

	d.ctx.storeMeta.regions[d.regionId] = d.Region()  // 更新老 Region 缓存
	d.ctx.storeMeta.regions[newRegion.Id] = newRegion // 添加新 Region 缓存
	// 将更新之后的指针位置region重新插入B树索引表中
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})

	d.ctx.storeMeta.Unlock()
	// 这个peer就是我们之前的封装咯
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		// 这里的错误通常是致命的，Panic 也许是更好的选择
		log.Errorf("create new peer failed: %v", err)
		return
	}
	// 路由表注册一下
	d.ctx.router.register(newPeer)
	// 这里是要发送一个tickerDriver的启动，启动新Region的心脏
	_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
	log.Infof("Split success! Old: %v, New: %v", d.Region(), newRegion)

	// 通知 Scheduler 更新路由信息 (Heartbeat)
	// 可以在这里手动触发一次 Heartbeat，或者等下一次 Tick
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		// 也要帮新 Region 报个到吗？新 Peer 启动后自己会报的
	}
}

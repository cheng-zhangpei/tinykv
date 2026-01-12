// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// Schedule 整个调度的过程写成一个调度循环
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// 1.找到合适的node，将最大的region的peer迁移到这个node上
	// Suitable: Up && DownTime < MaxStoreDownTime
	stores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		// downTime is the gap between now and lastHeartBeat
		// GetMaxStoreDownTime is the th
		// so this step is to select the alive store
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	if len(stores) <= 1 {
		return nil
	}
	// 排序
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})
	// 2.需要选择待迁移store中需要迁移的peer
	var region *core.RegionInfo
	var source *core.StoreInfo
	var target *core.StoreInfo
	for _, store := range stores {
		// 从最慢的store选择peer
		// 2.1 pending peer
		cluster.GetPendingRegionsWithLock(store.GetID(), func(c core.RegionsContainer) {
			region = c.RandomRegion(nil, nil) // 这是一个回调的函数
		})
		// 2.2 尝试是follower的peer
		if region == nil {
			cluster.GetFollowersWithLock(store.GetID(), func(c core.RegionsContainer) {
				region = c.RandomRegion(nil, nil)
			})
		}
		// 2.3 选择leader的peer
		if region == nil {
			cluster.GetLeadersWithLock(store.GetID(), func(c core.RegionsContainer) {
				region = c.RandomRegion(nil, nil)
			})
		}
		// 如果找到了一个peer，那就定下来了
		if region == nil {
			continue
		}
		// 状态不健康，这个region的状态不健康，副本的数量不够，这个时候我们要做的不是去搬动region而是去补region
		if len(region.GetPeers()) < cluster.GetMaxReplicas() {
			region = nil // 还没补齐副本，不要搬
			continue
		}

		source = store
		// 开始找target
		for i := len(stores) - 1; i >= 0; i-- {
			candidate := stores[i]
			// 基本检查
			if candidate.GetID() == source.GetID() {
				// 因为 target 是从小到大排的，如果撞到了 source，说明剩下的比 source 还大，没必要看了
				break
			}
			// 副本冲突检查 (Target 上不能已经有这个 Region 的副本)
			if region.GetStorePeer(candidate.GetID()) != nil {
				continue
			}
			// Diff 防抖检查
			// 只有这里通过了，才说明这个 Target 是值得搬运的
			if source.GetRegionSize()-candidate.GetRegionSize() > 2*region.GetApproximateSize() {
				target = candidate
				peer, err := cluster.AllocPeer(target.GetID())
				if err != nil {
					log.Errorf("scheduler can not alloc peer in store %d", target.GetID())
					return nil
				}
				op, err := operator.CreateMovePeerOperator(
					"balance-region",   // name
					cluster,            // cluster
					region,             // region
					operator.OpBalance, // kind
					source.GetID(),     // oldStore
					target.GetID(),     // newStore
					peer.GetId(),       // newPeerID
				)
				if err != nil {
					log.Errorf("operator can not create move peer in store %d", target.GetID())
					return nil
				}
				return op
			}
		}

	}
	return nil

}

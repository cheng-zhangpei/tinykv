package raft_storage

import (
	"context"
	"github.com/pingcap-incubator/tinykv/log"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/util/worker"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap/errors"
)

// Handle will resolve t's storeID into the address of the TinyKV node which should handle t. t's callback is then
// called with that address.
func (r *resolverRunner) Handle(t worker.Task) {
	log.Infof("***** TRACE 4: Runner received task")
	data := t.(*resolveAddrTask)
	data.callback(r.getAddr(data.storeID))
}

const storeAddressRefreshSeconds = 60

type storeAddr struct {
	addr       string
	lastUpdate time.Time
}

type resolverRunner struct {
	schedulerClient scheduler_client.Client
	storeAddrs      map[uint64]storeAddr
}

type resolveAddrTask struct {
	storeID  uint64
	callback func(addr string, err error)
}

func newResolverRunner(schedulerClient scheduler_client.Client) *resolverRunner {
	return &resolverRunner{
		schedulerClient: schedulerClient,
		storeAddrs:      make(map[uint64]storeAddr),
	}
}

func (r *resolverRunner) getAddr(id uint64) (string, error) {
	log.Infof("***** TRACE 6:enter the runner of sending msg")
	if sa, ok := r.storeAddrs[id]; ok {
		if time.Since(sa.lastUpdate).Seconds() < storeAddressRefreshSeconds {
			return sa.addr, nil
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store, err := r.schedulerClient.GetStore(ctx, id)
	log.Infof("***** TRACE 5(resolverRunner.getAddr): PD returned: store=%v, %v", store, err)
	if err != nil {
		return "", err
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return "", errors.Errorf("store %d has been removed", id)
	}
	addr := store.GetAddress()
	if addr == "" {
		return "", errors.Errorf("invalid empty address for store %d", id)
	}
	r.storeAddrs[id] = storeAddr{
		addr:       addr,
		lastUpdate: time.Now(),
	}
	return addr, nil
}

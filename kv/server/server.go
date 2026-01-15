package server

import (
	"context"
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage
	// (Used in 4B)
	Latches *latches.Latches
	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := &kvrpcpb.GetResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	// 在读的时候一定要先检查锁，不然可能会脏读
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	// 有锁就丢给客户端让客户端重新发
	if lock != nil {
		resp.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

// KvPrewrite 将req的操作放到暂存区，放入之前需要先加锁，等commit之后才将
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := &kvrpcpb.PrewriteResponse{}

	// 1. 获取所有 Key 并加锁 (Latches)
	// 防止并发请求同时修改内存中的状态导致竞态
	var keys [][]byte
	for _, m := range req.Mutations {
		keys = append(keys, m.Key)
	}
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, m := range req.Mutations {
		key := m.Key
		// 检查是否存在写写冲突
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		// 在StartVersion之后有对Key进行过修改则发生冲突了
		if write != nil && commitTs > req.StartVersion {
			resp.Errors = []*kvrpcpb.KeyError{{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTs,
					Key:        m.Key,
					Primary:    req.PrimaryLock,
				},
			}}
			// 只要有冲突那么整个batch就直接失败
			return resp, nil
		}
		// 检查是否有锁的冲突
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			resp.Errors = []*kvrpcpb.KeyError{{
				Locked: lock.Info(m.Key),
			}}
			return resp, nil
		}
		// 开始写入暂存区
		// 1、先加锁，说明现在数据正在改变
		// 2、再存入暂存区
		lock = &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(m.Op),
		}
		txn.PutLock(key, lock)
		//
		if m.Op == kvrpcpb.Op_Put {
			txn.PutValue(m.Key, m.Value)
		} else if m.Op == kvrpcpb.Op_Del {
			txn.DeleteValue(m.Key)
		}
	}
	// 这里需要注意一个点，就是我们TinyKV的暂存区是在磁盘中的，所以要有一个写操作
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		// 如果有问题就是这个region的问题
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := &kvrpcpb.CommitResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp = &kvrpcpb.CommitResponse{
				RegionError: regionErr.RequestErr,
			}
			return resp, err
		}
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		// 先判断是否有锁
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			// 可能之前已经提交了，这个时候去查一下是否已经提交 --> 这里的本质就是幂等性，就是防止重放
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}
			// 看看这个write的类型
			if write != nil {
				// 如果不是rollBack就说明成功了
				if write.Kind != mvcc.WriteKindRollback {
					continue
				}
				// 如果是 Rollback，说明被回滚了，那是真失败了
				resp.Error = &kvrpcpb.KeyError{Retryable: "write conflict: rollback"}
				return resp, nil
			}
		}
		// 不是我的锁
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "lock startTs mismatch"}
			return resp, nil
		}
		// 生成commit并写入
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		// 删除锁
		txn.DeleteLock(key)
	}
	// 前面的内容和之前一样都是在内存中，这里要写到磁盘里面去
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

/*
KvScan
像 KvGet 一样读取数据，但是是范围读取 (Range Scan)。
它必须遵循 MVCC 的可见性规则（只读 CommitTS <= ReadTS 的数据）。
它必须处理锁（遇到锁要报错）。
*/
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp = &kvrpcpb.ScanResponse{
				RegionError: regionErr.RequestErr,
			}
			return resp, err
		}
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	var pairs []*kvrpcpb.KvPair
	// 这里存储需要获得这个事务的前limit个key
	limit := int(req.Limit)
	for i := 0; i < limit; i++ {
		key, value, err := scanner.Next()
		if err != nil {
			// 注意：Scan 过程中遇到 Lock 错误，应该把错误记录在 Pair 里，而不是直接返回 error
			// 实际上，如果 scanner.Next 返回的是 KeyError，我们需要把它包装进 KvPair 发回去
			if keyErr, ok := err.(*kvrpcpb.KeyError); ok {
				pairs = append(pairs, &kvrpcpb.KvPair{
					Error: keyErr,
					Key:   key,
				})
				continue
			}
		}
		if key == nil {
			break
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})

	}
	resp.Pairs = pairs
	return resp, nil
}

/*
KvCheckTxnStatus 查询：Primary Key (主键) 到底提交了没？还在运行吗？这往往是一个事务遇到锁之后会去问主键的内容
决断：如果 Primary Key 的锁超时了 (TTL 过期)，直接把它回滚 (Rollback)，宣布这个事务死亡。
防重放：如果锁没了，也没提交记录，也要写一个 Rollback 记录，防止这个事务以后“诈尸”。
*/
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	keys := make([][]byte, 0)
	keys = append(keys, req.PrimaryKey)
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	// 1、 先看看是不是已经提交了
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.CurrentTs)
	write, commitTS, err := txn.CurrentWrite(req.PrimaryKey)
	if write != nil {
		// 事务已经提交了
		if write.Kind != mvcc.WriteKindRollback {
			// 已经提交了！告诉 Client提交的时间戳,如果是提交那么这个字段默认为 0
			resp.CommitVersion = commitTS
		}
		return resp, nil
	}
	// 没有提交就开始查锁
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	// 是否持有锁这两种情况又分为很多不一样的类型
	if lock != nil {
		// 既没 Commit，也没 Lock。说明锁丢了（可能被回滚了但 Write CF 被 GC 了？或者压根没 Prewrite 成功？）
		// 或者是 TTL 超时被别人清了？
		// 为了安全，我们必须在这里补一个 Rollback Record！防止它以后又诈尸提交。
		// 写入 Rollback Record
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		// 写盘 & 返回
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
	}
	// 锁还在就要检查TTL了
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey) // 把 Prewrite 的数据也删了
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		// 写盘
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		return resp, nil
	}
	// 锁还在并且还没有超时这个时候就把数据丢给client告诉他等着，这里还有事务没有搞定
	resp.LockTtl = lock.Ttl
	return resp, nil
}

/*
KvBatchRollback
删除 Lock CF 里的锁。
删除 Default CF 里的数据 (Prewrite 的数据)。
最重要：在 Write CF 里写一个 Rollback 记录 (墓碑)。
*/
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := &kvrpcpb.BatchRollbackResponse{}
	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	// 1、 先看看是不是已经提交了
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// 这里进来的一批Key全部进行rollBack
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue // 已经回滚过了，幂等成功
			} else {
				// 试图回滚一个已经提交的事务？报错！
				resp.Error = &kvrpcpb.KeyError{Abort: "true"} // Abort 表示严重错误
				return resp, nil
			}
		}
		lock, err := txn.GetLock(key)
		if lock != nil {
			if lock.Ts == req.StartVersion {
				// 是我的锁，删掉！
				txn.DeleteLock(key)
				txn.DeleteValue(key)
			} else {
				// 这里的逻辑：如果锁不是我的，但我接到了 Rollback 命令
				// 说明我的锁可能已经被别人清理了，或者我被覆盖了？
				// 无论如何，我要写入一个 Rollback Record 确保我这个 StartTS 以后无效
				// (继续往下走，写 Write CF)
			}
		}
		// 3. 写入 Rollback Record
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

/*
KvResolveLock
一旦通过 CheckTxnStatus 确定了 Primary Key 的最终命运（是 Commit 还是 Rollback）。
这个函数负责批量清理该事务留下的所有其他锁（Secondary Locks）。
要么全部提交 (Commit)，要么全部回滚 (Rollback)。

所以这个函数本质上就是已经有了主键的状态的时候，其他的键需要怎么搞
*/
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := &kvrpcpb.ResolveLockResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	var keys [][]byte
	// 遍历迭代器
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, _ := item.Value()
		lock, _ := mvcc.ParseLock(val)
		// 找一下请求的这个startTs这把锁的key，在这个Store中的锁
		if lock.Ts == req.StartVersion {
			keys = append(keys, item.KeyCopy(nil))
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}
	// ok 现在有两种操作, 根据主键状态去决定Commit()还是RollBack()
	if req.CommitVersion == 0 {
		// 这个是RollBack
		batchReq := &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		}
		// 复用 KvBatchRollback
		rbResp, err := server.KvBatchRollback(context.TODO(), batchReq)
		if err != nil {
			return nil, err
		}
		resp.RegionError = rbResp.RegionError
		resp.Error = rbResp.Error
	} else {
		// 这个是主键被Commit了，
		commitReq := &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			CommitVersion: req.CommitVersion,
			Keys:          keys,
		}
		// 复用 KvCommit
		cmResp, err := server.KvCommit(context.TODO(), commitReq)
		if err != nil {
			return nil, err
		}
		resp.RegionError = cmResp.RegionError
		resp.Error = cmResp.Error
	}
	return resp, nil
}

// Coprocessor SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		var regionErr *raft_storage.RegionError
		if errors.As(err, &regionErr) {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

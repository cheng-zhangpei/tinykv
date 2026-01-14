package server

import (
	"context"
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"

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
	// Your Code Here (4B).
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

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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

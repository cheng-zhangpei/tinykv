package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"math"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn     *MvccTxn
	iter    engine_util.DBIterator // 这是一个write区的迭代器
	nextKey []byte

	prefix []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	prefix := []byte(engine_util.CfWrite + "_")

	return &Scanner{
		txn:     txn,
		iter:    txn.Reader.IterCF(engine_util.CfWrite),
		nextKey: startKey,
		prefix:  prefix,
	}
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

func NextKey(key []byte) []byte {
	next := make([]byte, len(key)+1)
	copy(next, key)
	next[len(key)] = 0 // append 0
	return next
}

// Next returns the next key/value pair from the scanner.
// If the scanner is exhausted, then it will return `nil, nil, nil`.
// 说白了这个函数就是返回一个事务存在于Store的所有key
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// 每次调用都重新 Seek 到 nextKey，防止之前的迭代器状态干扰
	seekKey := EncodeKey(scan.nextKey, math.MaxUint64)
	scan.iter.Seek(seekKey)

	for {
		if !scan.iter.Valid() {
			return nil, nil, nil
		}

		item := scan.iter.Item()
		key := item.KeyCopy(nil)
		userKey := DecodeUserKey(key)

		// 记录当前正在处理的 UserKey
		currentUserKey := userKey

		// 检查 CommitTS
		commitTS := DecodeTimestamp(key)
		if commitTS > scan.txn.StartTS {
			// 版本太新，不可见。
			// 我们需要找当前 UserKey 的旧版本，所以只做 Next
			scan.iter.Next()
			continue
		}

		// 找到了 <= StartTS 的版本！
		// 无论它是 Put 还是 Delete，这个 UserKey 的处理到此为止。
		// 准备好下一次调用的起点：跳过当前 UserKey
		scan.nextKey = NextKey(currentUserKey)

		// 解析 Write Record
		val, err := item.Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, nil, err
		}

		// 如果是 Delete 或 Rollback
		if write.Kind != WriteKindPut {
			// 这个 Key 对用户不可见。
			// 我们必须跳过这个 UserKey 的所有剩余旧版本，直接去找下一个 UserKey。
			// 使用 nextKey 重新 Seek
			seekKey = EncodeKey(scan.nextKey, math.MaxUint64)
			scan.iter.Seek(seekKey)
			continue
		}

		// 如果是 Put
		// 1. 检查 Lock (必须检查，否则无法实现 SI)
		lock, err := scan.txn.GetLock(currentUserKey)
		if err != nil {
			return nil, nil, err
		}
		if lock != nil && lock.Ts <= scan.txn.StartTS {
			return nil, nil, &kvrpcpb.KeyError{Locked: lock.Info(currentUserKey)}
		}

		// 2. 获取 Value
		value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(currentUserKey, write.StartTS))
		if err != nil {
			return nil, nil, err
		}

		return currentUserKey, value, nil
	}
}
func (scan *Scanner) DebugDump() {
	iter := scan.txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// 用空 Key 来 Seek 到最前面
	for iter.Seek([]byte{}); iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		ts := DecodeTimestamp(item.Key()) // 注意函数名大小写
		val, _ := item.Value()
		write, _ := ParseWrite(val)

		log.Infof("DUMP: UserKey=%v, CommitTS=%d, Kind=%v, StartTS=%d",
			userKey, ts, write.Kind, write.StartTS)
	}
}

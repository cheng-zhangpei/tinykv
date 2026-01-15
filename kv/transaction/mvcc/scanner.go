package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn     *MvccTxn
	iter    engine_util.DBIterator // 这是一个write区的迭代器
	nextKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	return &Scanner{
		txn:     txn,
		iter:    txn.Reader.IterCF(engine_util.CfWrite),
		nextKey: startKey,
	}
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner.
// If the scanner is exhausted, then it will return `nil, nil, nil`.
// 说白了这个函数就是返回一个事务存在于Store的所有key
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// 这里写for循环的原因是有些write是delete类型的，我要删除这里的delete类型
	for {
		// Write中有记录,这个seek是往前找，小于StartTs的最新的commit日志哦
		scan.iter.Seek(EncodeKey(scan.nextKey, scan.txn.StartTS))
		if !scan.iter.Valid() {
			return nil, nil, nil
		}
		item := scan.iter.Item()
		key := item.KeyCopy(nil)
		userKey := DecodeUserKey(key)
		nextUserKey := append([]byte{}, userKey...) // 拷贝一份
		nextUserKey = append(nextUserKey, 0)        // 加一个 0 字节
		// 更新 Scanner 的状态，下次 Seek 用这个，这里是跳过了所有userKey的不同版本，在Store中，一个key的相同版本是并列放在一起的
		// 这个并列估计是底层LSM树的索引的特性
		scan.nextKey = nextUserKey

		// 用userKey去拿锁
		lock, err := scan.txn.GetLock(userKey)
		if err != nil {
			return nil, nil, err
		}
		if lock != nil && lock.Ts <= scan.txn.StartTS {
			// 说明这个事务已经commit，但是还没有释放锁或者还有其他的事务在修改
			return nil, nil, &kvrpcpb.KeyError{Locked: lock.Info(userKey)}
		}
		// 没有锁
		val, err := item.Value()
		if err != nil {
			return nil, nil, err
		}
		// 将Write区中的数据解析一下
		write, err := ParseWrite(val)

		if err != nil {
			return nil, nil, err
		}
		if write.Kind != WriteKindPut {
			continue
		}
		// 这里说明这个数据是这个事务存在于这个数据库的数据了

		value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
		if err != nil {
			return nil, nil, err
		}

		return userKey, value, nil
	}

}

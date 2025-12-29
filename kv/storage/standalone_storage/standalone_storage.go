package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance.
type StandAloneStorage struct {
	db *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := path.Join(dbPath, "kv")
	raftPath := path.Join(dbPath, "raft")

	kvDB := engine_util.CreateDB(kvPath, false)

	engines := engine_util.NewEngines(kvDB, nil, kvPath, raftPath)

	return &StandAloneStorage{
		db: engines,
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {

	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// update:false mean the Txn can not write, only read the data
	txn := s.db.Kv.NewTransaction(false)
	return &StandAloneReader{kvTxn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, operation := range batch {
		switch operation.Data.(type) {
		case storage.Put:
			putOps := operation.Data.(storage.Put)
			err := engine_util.PutCF(s.db.Kv, putOps.Cf, putOps.Key, putOps.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			deleteOps := operation.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.db.Kv, deleteOps.Cf, deleteOps.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		kvTxn: txn,
	}
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.kvTxn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.kvTxn)
}

func (r *StandAloneReader) Close() {
	r.kvTxn.Discard()
}

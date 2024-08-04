package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	var err error
	sas := new(StandAloneStorage)
	opt := badger.DefaultOptions
	opt.Dir = conf.DBPath
	opt.ValueDir = conf.DBPath
	sas.db, err = badger.Open(opt)
	if err != nil {
		panic(err)
	}
	return sas
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(_ *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return storageReader{txn}, nil
}

func (s *StandAloneStorage) Write(_ *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch data := m.Data.(type) {
			case storage.Put:
				key := engine_util.KeyWithCF(data.Cf, data.Key)
				err := txn.Set(key, data.Value)
				if err != nil {
					return err
				}
			case storage.Delete:
				key := engine_util.KeyWithCF(data.Cf, data.Key)
				err := txn.Delete(key)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type storageReader struct {
	txn *badger.Txn
}

func (s storageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := s.txn.Get(engine_util.KeyWithCF(cf, key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return item.Value()
}
func (s storageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}
func (s storageReader) Close() {
	s.txn.Discard()
}

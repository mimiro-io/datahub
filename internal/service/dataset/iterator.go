package dataset

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v3"

	"github.com/mimiro-io/datahub/internal/service/store"
)

type IterableDataset interface {
	At(since uint64) (Iterator, error)
}

type IterableBadgerDataset struct {
	db        *badger.DB
	datasetID uint32
}
type Iterator interface {
	io.Closer
	Inverse() Iterator
	LatestOnly() Iterator
	Next() bool
	Item() []byte
	NextOffset() uint64
	Error() error
}
type BadgerDatasetIterator struct {
	db             *badger.DB
	err            error
	txn            *badger.Txn
	it             *badger.Iterator
	datasetID      uint32
	startingOffset uint64
	datasetPrefix  []byte
	item           []byte
	offset         uint64
	inverse        bool
	latestOnly     bool
}

func (b *BadgerDatasetIterator) Close() error {
	if b.txn != nil {
		defer b.txn.Discard()
	}
	if b.it != nil {
		b.it.Close()
	}
	return nil
}

func (b *BadgerDatasetIterator) Inverse() Iterator {
	if b.txn != nil || b.it != nil {
		b.Close()
	}
	b.inverse = true
	if b.startingOffset == 0 {
		b.startingOffset = uint64(18446744073709551615) //max value, 0xFF,0xFF,0xFF,0xFF
	}
	return b
}

func (b *BadgerDatasetIterator) LatestOnly() Iterator {
	if b.txn != nil || b.it != nil {
		b.Close()
	}
	b.latestOnly = true
	return b
}

func (b *BadgerDatasetIterator) Next() bool {
	err := b.ensureTxn()
	if err != nil {
		b.err = err
		return false
	}
	if b.it.ValidForPrefix(b.datasetPrefix) {
		item := b.it.Item()
		k := item.Key()
		b.offset = binary.BigEndian.Uint64(k[6:])
		err := item.Value(func(key []byte) error {
			entityItem, err := b.txn.Get(key)
			if err != nil {
				return err
			}
			return entityItem.Value(func(jsonVal []byte) error {
				b.item = jsonVal
				return nil
			})
		})
		if err != nil {
			b.err = err
			return false
		}
		b.it.Next()
		return true
	}

	return false
}

func (b *BadgerDatasetIterator) ensureTxn() error {
	if b.txn == nil {
		b.txn = b.db.NewTransaction(false)
		b.datasetPrefix = store.SeekDataset(b.datasetID)
		opts := badger.DefaultIteratorOptions
		searchBuffer := store.SeekChanges(b.datasetID, b.startingOffset)
		if b.inverse {
			opts.Reverse = true
		}
		b.it = b.txn.NewIterator(opts)
		b.it.Seek(searchBuffer)
	}
	return nil
}

func (b *BadgerDatasetIterator) Item() []byte {
	return b.item
}

func (b *BadgerDatasetIterator) NextOffset() uint64 {
	if b.item == nil {
		return b.startingOffset
	}
	if b.inverse {
		return b.offset
	}
	return b.offset + 1
}

func (b *BadgerDatasetIterator) Error() error {
	return b.err
}

func (d IterableBadgerDataset) At(since uint64) (Iterator, error) {
	return &BadgerDatasetIterator{
		db:             d.db,
		datasetID:      d.datasetID,
		startingOffset: since,
	}, nil
}

func Of(store store.BadgerStore, datasetName string) (IterableDataset, error) {
	id, b := store.LookupDatasetId(datasetName)
	if b {
		return IterableBadgerDataset{store.GetDB(), id}, nil
	}
	return nil, fmt.Errorf("dataset %v not found", datasetName)
}

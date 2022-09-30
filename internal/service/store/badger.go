package store

import "github.com/dgraph-io/badger/v3"

type BadgerStore interface {
	GetDB() *badger.DB
	LookupDatasetId(datasetName string) (uint32, bool)
}

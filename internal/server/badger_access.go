package server

import "github.com/dgraph-io/badger/v3"

type BadgerAccess struct {
	b   *badger.DB
	dsm *DsManager
}

func (b BadgerAccess) GetDB() *badger.DB {
	return b.b
}

func (b BadgerAccess) LookupDatasetId(datasetName string) (uint32, bool) {
	ds := b.dsm.GetDataset(datasetName)
	if ds == nil {
		return 0, false
	}
	return ds.InternalID, true
}

func NewBadgerAccess(s *Store, dsm *DsManager) BadgerAccess {
	return BadgerAccess{s.database, dsm}
}

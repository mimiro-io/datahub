package store

import (
	"encoding/binary"

	"github.com/mimiro-io/datahub/internal/server"
)

func SeekChanges(datasetID uint32, since uint64) []byte {
	searchBuffer := make([]byte, 14)
	binary.BigEndian.PutUint16(searchBuffer, server.DATASET_ENTITY_CHANGE_LOG)
	binary.BigEndian.PutUint32(searchBuffer[2:], datasetID)
	binary.BigEndian.PutUint64(searchBuffer[6:], since)
	return searchBuffer
}

func SeekDataset(datasetID uint32) []byte {
	searchBuffer := make([]byte, 6)
	binary.BigEndian.PutUint16(searchBuffer, server.DATASET_ENTITY_CHANGE_LOG)
	binary.BigEndian.PutUint32(searchBuffer[2:], datasetID)
	return searchBuffer
}

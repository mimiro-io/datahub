// Copyright 2022 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

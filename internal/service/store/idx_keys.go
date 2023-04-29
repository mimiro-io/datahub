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
	"github.com/mimiro-io/datahub/internal/service/types"
)

func SeekChanges(datasetID types.InternalDatasetID, since types.DatasetOffset) []byte {
	searchBuffer := make([]byte, 14)
	binary.BigEndian.PutUint16(searchBuffer, server.DatasetEntityChangeLog)
	binary.BigEndian.PutUint32(searchBuffer[2:], uint32(datasetID))
	binary.BigEndian.PutUint64(searchBuffer[6:], uint64(since))
	return searchBuffer
}

func SeekEntityChanges(datasetID types.InternalDatasetID, entityID types.InternalID) []byte {
	entityIDBuffer := make([]byte, 14)
	binary.BigEndian.PutUint16(entityIDBuffer, server.EntityIDToJSONIndexID)
	binary.BigEndian.PutUint64(entityIDBuffer[2:], uint64(entityID))
	binary.BigEndian.PutUint32(entityIDBuffer[10:], uint32(datasetID))
	return entityIDBuffer
}

func SeekDataset(datasetID types.InternalDatasetID) []byte {
	searchBuffer := make([]byte, 6)
	binary.BigEndian.PutUint16(searchBuffer, server.DatasetEntityChangeLog)
	binary.BigEndian.PutUint32(searchBuffer[2:], uint32(datasetID))
	return searchBuffer
}

func SeekEntity(intenalEntityID types.InternalID) []byte {
	entityLocatorPrefixBuffer := make([]byte, 10)
	binary.BigEndian.PutUint16(entityLocatorPrefixBuffer, server.EntityIDToJSONIndexID)
	binary.BigEndian.PutUint64(entityLocatorPrefixBuffer[2:], uint64(intenalEntityID))
	return entityLocatorPrefixBuffer
}

func GetCurieKey(curie types.CURIE) []byte {
	curieAsBytes := []byte(curie)
	buf := make([]byte, len(curieAsBytes)+2)
	binary.BigEndian.PutUint16(buf, server.URIToIDIndexID)
	copy(buf[2:], curieAsBytes)
	return buf
}

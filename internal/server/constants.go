// Copyright 2021 MIMIRO AS
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

package server

import "encoding/binary"

type CollectionIndex uint16

const (
	URI_TO_ID_INDEX_ID         uint16 = 0
	ENTITY_ID_TO_JSON_INDEX_ID uint16 = 1
	INCOMING_REF_INDEX         uint16 = 2
	OUTGOING_REF_INDEX         uint16 = 3
	DATASET_ENTITY_CHANGE_LOG  uint16 = 4
	SYS_DATASETS_ID            uint16 = 5
	SYS_JOBS_ID                uint16 = 6
	SYS_DATASETS_SEQUENCES     uint16 = 7
	DATASET_LATEST_ENTITIES    uint16 = 8
	ID_TO_URI_INDEX_ID         uint16 = 9

	STORE_META_INDEX      CollectionIndex = 10
	NAMESPACES_INDEX      CollectionIndex = 11
	JOB_RESULT_INDEX      CollectionIndex = 12
	JOB_DATA_INDEX        CollectionIndex = 13
	JOB_CONFIGS_INDEX     CollectionIndex = 14
	CONTENT_INDEX         CollectionIndex = 15
	STORE_NEXT_DATASET_ID CollectionIndex = 16
	LOGIN_PROVIDER_INDEX  CollectionIndex = 17
)

var (
	JOB_RESULT_INDEX_BYTES      = uint16ToBytes(JOB_RESULT_INDEX)
	JOB_CONFIGS_INDEX_BYTES     = uint16ToBytes(JOB_CONFIGS_INDEX)
	CONTENT_INDEX_BYTES         = uint16ToBytes(CONTENT_INDEX)
	STORE_NEXT_DATASET_ID_BYTES = uint16ToBytes(STORE_NEXT_DATASET_ID)
	LOGIN_PROVIDER_INDEX_BYTES  = uint16ToBytes(LOGIN_PROVIDER_INDEX)
)

func uint16ToBytes(i CollectionIndex) []byte {
	indexBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(indexBytes, uint16(i))
	return indexBytes
}

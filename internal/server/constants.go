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
	URIToIDIndexID         uint16 = 0
	EntityIDToJSONIndexID  uint16 = 1
	IncomingRefIndex       uint16 = 2
	OutgoingRefIndex       uint16 = 3
	DatasetEntityChangeLog uint16 = 4
	SysDatasetsID          uint16 = 5
	SysJobsID              uint16 = 6
	SysDatasetsSequences   uint16 = 7
	DatasetLatestEntities  uint16 = 8
	IDToURIIndexID         uint16 = 9

	StoreMetaIndex     CollectionIndex = 10
	NamespacesIndex    CollectionIndex = 11
	JobResultIndex     CollectionIndex = 12
	JobDataIndex       CollectionIndex = 13
	JobConfigIndex     CollectionIndex = 14
	ContentIndex       CollectionIndex = 15
	StoreNextDatasetID CollectionIndex = 16
	LoginProviderIndex CollectionIndex = 17
	JobMetaIndex       CollectionIndex = 18
)

var (
	JobResultIndexBytes     = uint16ToBytes(JobResultIndex)
	JobConfigsIndexBytes    = uint16ToBytes(JobConfigIndex)
	JobMetaIndexBytes       = uint16ToBytes(JobMetaIndex)
	ContentIndexBytes       = uint16ToBytes(ContentIndex)
	StoreNextDatasetIDBytes = uint16ToBytes(StoreNextDatasetID)
	LoginProviderIndexBytes = uint16ToBytes(LoginProviderIndex)
)

func uint16ToBytes(i CollectionIndex) []byte {
	indexBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(indexBytes, uint16(i))
	return indexBytes
}

func collectionToStr(idx uint16) string {
	switch idx {
	case URIToIDIndexID:
		return "URIToIDIndexID"
	case EntityIDToJSONIndexID:
		return "EntityIDToJSONIndexID"
	case IncomingRefIndex:
		return "IncomingRefIndex"
	case OutgoingRefIndex:
		return "OutgoingRefIndex"
	case DatasetEntityChangeLog:
		return "DatasetEntityChangeLog"
	case SysDatasetsID:
		return "SysDatasetsID"
	case SysJobsID:
		return "SysJobsID"
	case SysDatasetsSequences:
		return "SysDatasetsSequences"
	case DatasetLatestEntities:
		return "DatasetLatestEntities"
	case IDToURIIndexID:
		return "IDToURIIndexID"
	case uint16(StoreMetaIndex):
		return "StoreMetaIndex"
	case uint16(NamespacesIndex):
		return "NamespacesIndex"
	case uint16(JobResultIndex):
		return "JobResultIndex"
	case uint16(JobDataIndex):
		return "JobDataIndex"
	case uint16(JobConfigIndex):
		return "JobConfigIndex"
	case uint16(ContentIndex):
		return "ContentIndex"
	case uint16(StoreNextDatasetID):
		return "StoreNextDatasetID"
	case uint16(LoginProviderIndex):
		return "LoginProviderIndex"
	default:
		return "unknown"
	}

}

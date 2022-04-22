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

import (
	"github.com/DataDog/datadog-go/v5/statsd"
	"go.uber.org/zap"
	"os"
	"strconv"
	"sync"
	"testing"
)

func BenchmarkDatasetStoreEntities(b *testing.B) {
	storeLocation, peopleNamespacePrefix, companyNamespacePrefix, ds := setupBench()

	b.ReportAllocs()

	b.StopTimer()
	deleteMarker := false
	for no := 0; no < b.N; no++ {
		persons := make([]*Entity, 1000)
		for i := 0; i < cap(persons); i++ {
			person := NewEntity(peopleNamespacePrefix+":person"+strconv.Itoa(i), uint64(i))
			person.Properties[peopleNamespacePrefix+":Name"] = "person"
			person.References[peopleNamespacePrefix+":worksfor"] = companyNamespacePrefix + ":company-3"
			person.References[peopleNamespacePrefix+":workedfor"] = []string{
				companyNamespacePrefix + ":company-2",
				companyNamespacePrefix + ":company-1",
			}
			person.IsDeleted = deleteMarker
			persons[i] = person
		}
		deleteMarker = !deleteMarker
		b.StartTimer()
		err := ds.StoreEntities(persons)
		if err != nil {
			b.Fail()
		}
		b.StopTimer()
		persons = persons[:0] // clear out slice
	}
	ds.store.Close()
	os.RemoveAll(storeLocation)
}

func setupBench() (string, string, string, *Dataset) {
	storeLocation := "./test_store_entities_refs_lists"
	s := &Store{
		datasets:        &sync.Map{},
		deletedDatasets: make(map[uint32]bool),
		storeLocation:   storeLocation,
		logger:          zap.NewNop().Sugar(),
		statsdClient:    &statsd.NoOpClient{},
	}
	s.NamespaceManager = NewNamespaceManager(s)
	s.Open()

	dsm := &DsManager{
		store:  s,
		logger: zap.NewNop().Sugar(),
		eb:     NoOpBus(),
	}

	// if we are missing core datasets, we add these here
	dsm.CreateDataset(datasetCore, nil)

	peopleNamespacePrefix, _ := s.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
	companyNamespacePrefix, _ := s.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/company/")
	ds, _ := dsm.CreateDataset("people", nil)
	return storeLocation, peopleNamespacePrefix, companyNamespacePrefix, ds
}

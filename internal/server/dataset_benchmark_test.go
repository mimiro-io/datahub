package server

import (
	"github.com/DataDog/datadog-go/statsd"
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
	dsm.CreateDataset(datasetCore)

	peopleNamespacePrefix, _ := s.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
	companyNamespacePrefix, _ := s.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/company/")
	ds, _ := dsm.CreateDataset("people")
	return storeLocation, peopleNamespacePrefix, companyNamespacePrefix, ds
}

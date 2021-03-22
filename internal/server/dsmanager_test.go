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

/*
import (
	"fmt"
	"os"
	"testing"

	"github.com/franela/goblin"
)

func TestDatasetManager(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The dataset manager", func() {
		testCnt := 0
		var dsm *DsManager
		var store *Store
		var storeLocation string
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_dataset_manager_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			store, dsm = setupStore(storeLocation, t)
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("Should give correct dataset details after storage", func() {
			ds, err := dsm.CreateDataset("people")
			g.Assert(err).IsNil()
			g.Assert(ds).IsNotZero()

			details, found, err := dsm.GetDatasetDetails("people")
			g.Assert(err).IsNil()
			g.Assert(found).IsTrue()
			g.Assert(details).IsNotZero()
			g.Assert(details.ID).Eql("ns0:people")
			g.Assert(details.Properties).Eql(map[string]interface{}{"ns0:items": 0, "ns0:name": "people"})

			ds, err = dsm.CreateDataset("more.people")
			g.Assert(err).IsNil()
			g.Assert(ds).IsNotZero()

			err = ds.StoreEntities([]*Entity{&Entity{ID: "hei"}})
			g.Assert(err).IsNil()

			details, found, err = dsm.GetDatasetDetails("people")
			g.Assert(details.Properties).Eql(map[string]interface{}{"ns0:items": 0, "ns0:name": "people"},
				"people dataset was not unchanged")

			details, found, err = dsm.GetDatasetDetails("more.people")
			g.Assert(details.Properties).Eql(map[string]interface{}{"ns0:items": 1, "ns0:name": "more.people"})
		})
	})
}

func TestDeletedDatasetsStored(m *testing.T) {
	storeLocation := "./test_create_dataset_store"
	err := os.RemoveAll(storeLocation)
	checkFail(err, m)

	s, dsm := setupStore(storeLocation, m)

	ds, err := dsm.CreateDataset("people")
	checkFail(err, m)

	if ds == nil {
		m.FailNow()
	}

	err = dsm.DeleteDataset("people")
	checkFail(err, m)

	err = s.Close()
	checkFail(err, m)

	s1, _ := setupStore(storeLocation, m)

	if len(s1.deletedDatasets) != 1 {
		m.FailNow()
	}

	_, ok := s1.deletedDatasets[ds.InternalID]
	if !ok {
		m.FailNow()
	}
}

func TestDeleteDataset(m *testing.T) {

	storeLocation := "./test_create_dataset_store"
	err := os.RemoveAll(storeLocation)
	checkFail(err, m)

	_, dsm := setupStore(storeLocation, m)

	ds, err := dsm.CreateDataset("people")
	if ds == nil {
		m.FailNow()
	}

	_ = dsm.DeleteDataset("people")

	ds1, err := dsm.CreateDataset("people")
	if ds1 == nil {
		m.FailNow()
	}

	if ds1.InternalID == ds.InternalID {
		m.FailNow()
	}

	if ds1.ID != ds.ID {
		m.FailNow()
	}
}

func TestCreateDataset(m *testing.T) {

	storeLocation := "./test_create_dataset_store"

	err := os.RemoveAll(storeLocation)
	checkFail(err, m)

	s, dsm := setupStore(storeLocation, m)

	ds, err := dsm.CreateDataset("people")
	if ds == nil {
		m.FailNow()
	}

	if ds.InternalID != 2 {
		m.FailNow()
	}

	_ = s.Close()

	// open again
	s, dsm = setupStore(storeLocation, m)

	ds = dsm.GetDataset("people")
	if ds == nil {
		m.FailNow()
	}

	if ds.InternalID != 2 {
		m.FailNow()
	}

	ds2, err := dsm.CreateDataset("animals")
	if ds2 == nil {
		m.FailNow()
	}

	if ds2.InternalID != 3 {
		m.FailNow()
	}

	err = os.RemoveAll(storeLocation)
	checkFail(err, m)
}
*/

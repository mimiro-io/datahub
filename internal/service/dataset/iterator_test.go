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

package dataset_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	ds "github.com/mimiro-io/datahub/internal/service/dataset"
	"github.com/mimiro-io/datahub/internal/service/types"
)

func TestDatasetIterator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "service/Dataset Suite")
}

var _ = Describe("A dataset iterator", Ordered, func() {
	storeLocation := "./test_service_dataset_iterator"
	var lc *fxtest.Lifecycle
	var dsm *server.DsManager
	var ds0 *server.Dataset
	var ds1 *server.Dataset
	var ds2 *server.Dataset
	var store *server.Store
	BeforeAll(func() {
		os.RemoveAll(storeLocation)
		lc = fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))
		env := &conf.Env{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		store = server.NewStore(lc, env, &statsd.NoOpClient{})
		dsm = server.NewDsManager(lc, env, store, server.NoOpBus())
		lc.Start(context.Background())

		ds0, _ = dsm.CreateDataset("arabic", nil)
		ds0.StoreEntities([]*server.Entity{
			server.NewEntity("1", 0),
			server.NewEntity("2", 0),
			server.NewEntity("3", 0),
			server.NewEntity("4", 0),
		})
		ds1, _ = dsm.CreateDataset("roman", nil)
		ds1.StoreEntities([]*server.Entity{
			server.NewEntity("I", 0),
			server.NewEntity("II", 0),
			server.NewEntity("III", 0),
			server.NewEntity("IV", 0),
		})
		ds2, _ = dsm.CreateDataset("letters", nil)
		ds2.StoreEntities([]*server.Entity{
			server.NewEntity("a", 0),
			server.NewEntity("b", 0),
			server.NewEntity("c", 0),
			server.NewEntity("d", 0),
		})
	})
	AfterAll(func() {
		lc.Stop(context.Background())
		os.RemoveAll(storeLocation)
	})
	It("should be able to create a 0 offset", func() {
		dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
		Expect(err).To(BeNil())
		it, err := dataset.At(0)
		Expect(err).To(BeNil())
		Expect(it.Error()).To(BeNil())
		var continuationToken types.DatasetOffset = 0
		var foundIds []string
		for it.Next() {
			jsonData := it.Item()
			e := server.Entity{}
			err := json.Unmarshal(jsonData, &e)
			Expect(err).To(BeNil())
			foundIds = append(foundIds, e.ID)
			continuationToken = it.NextOffset()
		}
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(foundIds).To(Equal([]string{"I", "II", "III", "IV"}))
		Expect(continuationToken).To(Equal(types.DatasetOffset(4)))
	})
	It("should be able to create a 3 offset", func() {
		dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
		Expect(err).To(BeNil())
		it, err := dataset.At(3)
		Expect(err).To(BeNil())
		Expect(it.Error()).To(BeNil())
		var continuationToken types.DatasetOffset = 0
		var foundIds []string
		for it.Next() {
			jsonData := it.Item()
			e := server.Entity{}
			err := json.Unmarshal(jsonData, &e)
			Expect(err).To(BeNil())
			foundIds = append(foundIds, e.ID)
			continuationToken = it.NextOffset()
		}
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(foundIds).To(Equal([]string{"IV"}))
		Expect(continuationToken).To(Equal(types.DatasetOffset(4)))
	})
	It("should produce correct nextOffsets", func() {
		dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
		Expect(err).To(BeNil())
		it, err := dataset.At(0)
		Expect(err).To(BeNil())
		Expect(it.Error()).To(BeNil())
		var continuationToken types.DatasetOffset = 0
		var foundIds []string
		for it.Next() {
			jsonData := it.Item()
			e := server.Entity{}
			err2 := json.Unmarshal(jsonData, &e)
			Expect(err2).To(BeNil())
			foundIds = append(foundIds, e.ID)
			continuationToken = it.NextOffset()
			if len(foundIds) == 2 {
				break
			}
		}
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(foundIds).To(Equal([]string{"I", "II"}))
		Expect(continuationToken).To(Equal(types.DatasetOffset(2)))

		it, err = dataset.At(continuationToken)
		Expect(err).To(BeNil())
		foundIds = []string{}
		for it.Next() {
			jsonData := it.Item()
			e := server.Entity{}
			err := json.Unmarshal(jsonData, &e)
			Expect(err).To(BeNil())
			foundIds = append(foundIds, e.ID)
			continuationToken = it.NextOffset()
		}
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(foundIds).To(Equal([]string{"III", "IV"}))
		Expect(continuationToken).To(Equal(types.DatasetOffset(4)))
	})
	It("should be able to create a 4 offset", func() {
		dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
		Expect(err).To(BeNil())
		it, err := dataset.At(4)
		Expect(err).To(BeNil())
		Expect(it.Next()).To(BeFalse(), "nothing found")
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(it.NextOffset()).To(Equal(types.DatasetOffset(4)))
	})
	It("should not fail for too large offset, but emit nothing", func() {
		dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
		Expect(err).To(BeNil())
		it, err := dataset.At(5)
		Expect(err).To(BeNil())
		Expect(it.Next()).To(BeFalse(), "nothing found")
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(it.NextOffset()).To(Equal(types.DatasetOffset(5)))
	})
	It("should list inverse changes from 0", func() {
		dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
		Expect(err).To(BeNil())
		it, err := dataset.At(0)
		Expect(err).To(BeNil())
		it = it.Inverse()
		Expect(it.Error()).To(BeNil())
		var continuationToken types.DatasetOffset = 0
		var foundIds []string
		for it.Next() {
			jsonData := it.Item()
			e := server.Entity{}
			err := json.Unmarshal(jsonData, &e)
			Expect(err).To(BeNil())
			foundIds = append(foundIds, e.ID)
			continuationToken = it.NextOffset()
		}
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(foundIds).To(Equal([]string{"IV", "III", "II", "I"}))
		Expect(continuationToken).To(Equal(types.DatasetOffset(0)))
	})
	It("should produce correct inverse nextOffsets", func() {
		dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
		Expect(err).To(BeNil())
		it, err := dataset.At(0)
		it = it.Inverse()
		Expect(err).To(BeNil())
		Expect(it.Error()).To(BeNil())
		var continuationToken types.DatasetOffset = 0
		var foundIds []string
		for it.Next() {
			jsonData := it.Item()
			e := server.Entity{}
			err2 := json.Unmarshal(jsonData, &e)
			Expect(err2).To(BeNil())
			foundIds = append(foundIds, e.ID)
			continuationToken = it.NextOffset()
			if len(foundIds) == 2 {
				break
			}
		}
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(foundIds).To(Equal([]string{"IV", "III"}))
		Expect(continuationToken).To(Equal(types.DatasetOffset(2)))

		it, err = dataset.At(continuationToken)
		Expect(err).To(BeNil())
		it = it.Inverse()
		foundIds = []string{}
		for it.Next() {
			jsonData := it.Item()
			e := server.Entity{}
			err := json.Unmarshal(jsonData, &e)
			Expect(err).To(BeNil())
			foundIds = append(foundIds, e.ID)
			continuationToken = it.NextOffset()
		}
		it.Close()
		Expect(it.Error()).To(BeNil())
		Expect(foundIds).To(Equal([]string{"II", "I"}))
		Expect(continuationToken).To(Equal(types.DatasetOffset(0)))
	})
})

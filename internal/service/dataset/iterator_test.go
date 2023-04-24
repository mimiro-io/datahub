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
	"github.com/franela/goblin"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	ds "github.com/mimiro-io/datahub/internal/service/dataset"
	"github.com/mimiro-io/datahub/internal/service/types"
)

func TestDatasetIterator(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("A dataset iterator", func() {
		storeLocation := "./test_service_dataset_iterator"
		var lc *fxtest.Lifecycle
		var dsm *server.DsManager
		var ds0 *server.Dataset
		var ds1 *server.Dataset
		var ds2 *server.Dataset
		var store *server.Store
		g.Before(func() {
			os.RemoveAll(storeLocation)
			lc = fxtest.NewLifecycle(internal.FxTestLog(t, false))
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
		g.After(func() {
			lc.Stop(context.Background())
			os.RemoveAll(storeLocation)
		})
		g.It("should be able to create a 0 offset", func() {
			dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
			g.Assert(err).IsNil()
			it, err := dataset.At(0)
			g.Assert(err).IsNil()
			g.Assert(it.Error()).IsNil()
			var continuationToken types.DatasetOffset = 0
			var foundIds []string
			for it.Next() {
				jsonData := it.Item()
				e := server.Entity{}
				err := json.Unmarshal(jsonData, &e)
				g.Assert(err).IsNil()
				foundIds = append(foundIds, e.ID)
				continuationToken = it.NextOffset()
			}
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(foundIds).Equal([]string{"I", "II", "III", "IV"})
			g.Assert(continuationToken).Equal(types.DatasetOffset(4))
		})
		g.It("should be able to create a 3 offset", func() {
			dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
			g.Assert(err).IsNil()
			it, err := dataset.At(3)
			g.Assert(err).IsNil()
			g.Assert(it.Error()).IsNil()
			var continuationToken types.DatasetOffset = 0
			var foundIds []string
			for it.Next() {
				jsonData := it.Item()
				e := server.Entity{}
				err := json.Unmarshal(jsonData, &e)
				g.Assert(err).IsNil()
				foundIds = append(foundIds, e.ID)
				continuationToken = it.NextOffset()
			}
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(foundIds).Equal([]string{"IV"})
			g.Assert(continuationToken).Equal(types.DatasetOffset(4))
		})
		g.It("should produce correct nextOffsets", func() {
			dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
			g.Assert(err).IsNil()
			it, err := dataset.At(0)
			g.Assert(err).IsNil()
			g.Assert(it.Error()).IsNil()
			var continuationToken types.DatasetOffset = 0
			var foundIds []string
			for it.Next() {
				jsonData := it.Item()
				e := server.Entity{}
				err2 := json.Unmarshal(jsonData, &e)
				g.Assert(err2).IsNil()
				foundIds = append(foundIds, e.ID)
				continuationToken = it.NextOffset()
				if len(foundIds) == 2 {
					break
				}
			}
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(foundIds).Equal([]string{"I", "II"})
			g.Assert(continuationToken).Equal(types.DatasetOffset(2))

			it, err = dataset.At(continuationToken)
			g.Assert(err).IsNil()
			foundIds = []string{}
			for it.Next() {
				jsonData := it.Item()
				e := server.Entity{}
				err := json.Unmarshal(jsonData, &e)
				g.Assert(err).IsNil()
				foundIds = append(foundIds, e.ID)
				continuationToken = it.NextOffset()
			}
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(foundIds).Equal([]string{"III", "IV"})
			g.Assert(continuationToken).Equal(types.DatasetOffset(4))
		})
		g.It("should be able to create a 4 offset", func() {
			dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
			g.Assert(err).IsNil()
			it, err := dataset.At(4)
			g.Assert(err).IsNil()
			g.Assert(it.Next()).IsFalse("nothing found")
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(it.NextOffset()).Equal(types.DatasetOffset(4))
		})
		g.It("should not fail for too large offset, but emit nothing", func() {
			dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
			g.Assert(err).IsNil()
			it, err := dataset.At(5)
			g.Assert(err).IsNil()
			g.Assert(it.Next()).IsFalse("nothing found")
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(it.NextOffset()).Equal(types.DatasetOffset(5))
		})
		g.It("should list inverse changes from 0", func() {
			dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
			g.Assert(err).IsNil()
			it, err := dataset.At(0)
			g.Assert(err).IsNil()
			it = it.Inverse()
			g.Assert(it.Error()).IsNil()
			var continuationToken types.DatasetOffset = 0
			var foundIds []string
			for it.Next() {
				jsonData := it.Item()
				e := server.Entity{}
				err := json.Unmarshal(jsonData, &e)
				g.Assert(err).IsNil()
				foundIds = append(foundIds, e.ID)
				continuationToken = it.NextOffset()
			}
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(foundIds).Equal([]string{"IV", "III", "II", "I"})
			g.Assert(continuationToken).Equal(types.DatasetOffset(0))
		})
		g.It("should produce correct inverse nextOffsets", func() {
			dataset, err := ds.Of(server.NewBadgerAccess(store, dsm), ds1.ID)
			g.Assert(err).IsNil()
			it, err := dataset.At(0)
			it = it.Inverse()
			g.Assert(err).IsNil()
			g.Assert(it.Error()).IsNil()
			var continuationToken types.DatasetOffset = 0
			var foundIds []string
			for it.Next() {
				jsonData := it.Item()
				e := server.Entity{}
				err2 := json.Unmarshal(jsonData, &e)
				g.Assert(err2).IsNil()
				foundIds = append(foundIds, e.ID)
				continuationToken = it.NextOffset()
				if len(foundIds) == 2 {
					break
				}
			}
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(foundIds).Equal([]string{"IV", "III"})
			g.Assert(continuationToken).Equal(types.DatasetOffset(2))

			it, err = dataset.At(continuationToken)
			g.Assert(err).IsNil()
			it = it.Inverse()
			foundIds = []string{}
			for it.Next() {
				jsonData := it.Item()
				e := server.Entity{}
				err := json.Unmarshal(jsonData, &e)
				g.Assert(err).IsNil()
				foundIds = append(foundIds, e.ID)
				continuationToken = it.NextOffset()
			}
			it.Close()
			g.Assert(it.Error()).IsNil()
			g.Assert(foundIds).Equal([]string{"II", "I"})
			g.Assert(continuationToken).Equal(types.DatasetOffset(0))
		})
	})
}

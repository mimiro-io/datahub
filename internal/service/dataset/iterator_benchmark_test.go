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
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	ds "github.com/mimiro-io/datahub/internal/service/dataset"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"os"
	"strconv"
	"testing"
)

func BenchmarkChangesIterator(b *testing.B) {
	storeLocation := "./test_store_iterator_bench_2"
	dataset, store, dsm, after := setup(storeLocation, b)
	defer after()
	ds, _ := ds.Of(server.NewBadgerAccess(store, dsm), dataset.ID)
	b.ReportAllocs()
	b.ResetTimer()
	for no := 0; no < b.N; no++ {
		c := 0
		//b.Log("run")
		it, _ := ds.At(0)
		for it.Next() {
			c += len(it.Item())
		}
		it.Close()
		//b.Log(c)
		//b.Logf("found %v", raw)
	}
}
func BenchmarkGetChanges(b *testing.B) {
	storeLocation := "./test_store_iterator_bench_1"
	dataset, _, _, after := setup(storeLocation, b)
	defer after()
	b.ReportAllocs()
	b.ResetTimer()
	for no := 0; no < b.N; no++ {
		//b.Log("run")
		c := 0
		_, err := dataset.ProcessChangesRaw(0, 10000, false, func(jsonData []byte) error {
			c += len(jsonData)
			return nil
		})
		if err != nil {
			panic(err)
		}
		//b.Logf("found %v", raw)
		//b.Log(c)
	}
}

func setup(storeLocation string, b *testing.B) (*server.Dataset, *server.Store, *server.DsManager, func()) {
	lc := fxtest.NewLifecycle(internal.FxTestLog(b, false))
	env := &conf.Env{
		Logger:        zap.NewNop().Sugar(),
		StoreLocation: storeLocation,
	}
	store := server.NewStore(lc, env, &statsd.NoOpClient{})
	dsm := server.NewDsManager(lc, env, store, server.NoOpBus())
	lc.Start(context.Background())

	ds0, _ := dsm.CreateDataset("arabic", nil)
	for i := 1; i < 5000; i++ {
		ds0.StoreEntities([]*server.Entity{server.NewEntity(strconv.Itoa(i), 0)})
	}
	return ds0, store, dsm, func() {
		lc.Stop(context.Background())
		os.RemoveAll(storeLocation)
	}
}

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

package security

import (
	"os"
	"strconv"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/franela/goblin"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

func TestManager(t *testing.T) {
	g := goblin.Goblin(t)

	g.Describe("Security Manager", func() {
		var store *server.Store
		var e *conf.Env
		var pm *ProviderManager
		testCnt := 0

		g.BeforeEach(func() {
			testCnt = testCnt + 1
			e = &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: "./test_login_provider_" + strconv.Itoa(testCnt),
			}
			err := os.RemoveAll(e.StoreLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + e.StoreLocation)
			lc := fxtest.NewLifecycle(internal.FxTestLog(t, false))
			sc := &statsd.NoOpClient{}
			store = server.NewStore(lc, e, sc)
			lc.RequireStart()
			pm = NewProviderManager(lc, e, store, zap.NewNop().Sugar())
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(e.StoreLocation)
		})
		g.It("should add a config", func() {
			p := createConfig("test-jwt")
			err := pm.AddProvider(p)
			g.Assert(err).IsNil()
		})
	})
}

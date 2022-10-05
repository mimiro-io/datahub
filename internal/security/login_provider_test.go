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
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"os"
	"strconv"
	"testing"
)

func TestCrud(t *testing.T) {
	g := goblin.Goblin(t)

	g.Describe("Login Provider Crud operations", func() {

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
			g.Assert(err).IsNil("should be allowed to clean test files in " + e.StoreLocation)
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
		g.It("should be able to get a config by name", func() {
			p := createConfig("test-jwt-2")
			err := pm.AddProvider(p)
			g.Assert(err).IsNil()

			p2, err := pm.FindByName("test-jwt-2")
			g.Assert(err).IsNil()
			g.Assert(p2).IsNotNil()

			g.Assert(p2.Name).Equal("test-jwt-2")
		})
		g.It("should be able to update a config", func() {
			p := createConfig("test-jwt-3")
			err := pm.AddProvider(p)
			g.Assert(err).IsNil()

			p.Endpoint = &ValueReader{
				Type:  "text",
				Value: "http://localhost/hello",
			}
			err = pm.AddProvider(p)
			g.Assert(err).IsNil()

			// load it back
			p2, err := pm.FindByName("test-jwt-3")
			g.Assert(err).IsNil()
			g.Assert(p2).IsNotNil()
			g.Assert(pm.LoadValue(p2.Endpoint)).Equal("http://localhost/hello")

			_, err = pm.FindByName("test-jwt-4")
			g.Assert(err).Eql(ErrLoginProviderNotFound)

		})
		g.It("should be able to delete a config", func() {
			p := createConfig("test-jwt-5")
			err := pm.AddProvider(p)
			g.Assert(err).IsNil()

			p2, err := pm.FindByName("test-jwt-5")
			g.Assert(err).IsNil()
			g.Assert(p2).IsNotNil()

			err = pm.DeleteProvider("test-jwt-5")
			g.Assert(err).IsNil()

			p3, err := pm.FindByName("test-jwt-5")
			g.Assert(err).Eql(ErrLoginProviderNotFound)
			g.Assert(p3).IsZero()
		})
		g.It("should list all configs", func() {
			p := createConfig("test-jwt-6")
			_ = pm.AddProvider(p)
			p2 := createConfig("test-jwt-7")
			_ = pm.AddProvider(p2)

			items, err := pm.ListProviders()
			g.Assert(err).IsNil()
			g.Assert(len(items)).Equal(2)

		})
	})
}

func createConfig(name string) ProviderConfig {
	return ProviderConfig{
		Name: name,
		Type: "bearer",
		ClientId: &ValueReader{
			Type:  "text",
			Value: "id1",
		},
		ClientSecret: &ValueReader{
			Type:  "text",
			Value: "some-secret",
		},
		Audience: &ValueReader{
			Type:  "text",
			Value: "mimiro",
		},
		GrantType: &ValueReader{
			Type:  "text",
			Value: "test_grant",
		},
		Endpoint: &ValueReader{
			Type:  "text",
			Value: "http://localhost",
		},
	}
}

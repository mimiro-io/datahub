package security

import (
	"errors"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/conf/secrets"
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
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + e.StoreLocation)
			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(t)
			sc := &statsd.NoOpClient{}
			store = server.NewStore(lc, e, sc)
			sm := &secrets.NoopStore{}
			lc.RequireStart()
			os.Stderr = oldErr
			pm = NewProviderManager(lc, e, store, zap.NewNop().Sugar(), sm)
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
			err = pm.UpdateProvider("test-jwt-3", p)
			g.Assert(err).IsNil()

			// load it back
			p2, err := pm.FindByName("test-jwt-3")
			g.Assert(err).IsNil()
			g.Assert(p2).IsNotNil()
			g.Assert(pm.LoadValue(p2.Endpoint)).Equal("http://localhost/hello")

			err = pm.UpdateProvider("test-jwt-4", p)
			g.Assert(errors.Is(err, ErrLoginProviderNotFound)).IsTrue()

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
			g.Assert(err).IsNil()
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

package security

import (
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
	})
}

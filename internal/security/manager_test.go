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

	"github.com/DataDog/datadog-go/v5/statsd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

var _ = Describe("Security Manager", func() {
	var store *server.Store
	var e *conf.Env
	var pm *ProviderManager
	testCnt := 0

	BeforeEach(func() {
		testCnt = testCnt + 1
		e = &conf.Env{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: "./test_login_provider_" + strconv.Itoa(testCnt),
		}
		err := os.RemoveAll(e.StoreLocation)
		Expect(err).To(BeNil())
		// lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))
		sc := &statsd.NoOpClient{}
		store = server.NewStore(e, sc)
		// lc.RequireStart()
		pm = NewProviderManager(e, store, zap.NewNop().Sugar())
	})
	AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(e.StoreLocation)
	})
	It("should add a config", func() {
		p := createConfig("test-jwt")
		err := pm.AddProvider(p)
		Expect(err).To(BeNil())
	})
})

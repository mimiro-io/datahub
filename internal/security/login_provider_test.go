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

var _ = Describe("Login Provider Crud operations", func() {
	var store *server.Store
	var e *conf.Config
	var pm *ProviderManager
	testCnt := 0

	BeforeEach(func() {
		testCnt = testCnt + 1
		e = &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: "./test_login_provider_" + strconv.Itoa(testCnt),
		}
		err := os.RemoveAll(e.StoreLocation)
		Expect(err).To(BeNil(), "should be allowed to clean test files in "+e.StoreLocation)
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
	It("should be able to get a config by name", func() {
		p := createConfig("test-jwt-2")
		err := pm.AddProvider(p)
		Expect(err).To(BeNil())

		p2, err := pm.FindByName("test-jwt-2")
		Expect(err).To(BeNil())
		Expect(p2).NotTo(BeNil())

		Expect(p2.Name).To(Equal("test-jwt-2"))
	})
	It("should be able to update a config", func() {
		p := createConfig("test-jwt-3")
		err := pm.AddProvider(p)
		Expect(err).To(BeNil())

		p.Endpoint = &ValueReader{
			Type:  "text",
			Value: "http://localhost/hello",
		}
		err = pm.AddProvider(p)
		Expect(err).To(BeNil())

		// load it back
		p2, err := pm.FindByName("test-jwt-3")
		Expect(err).To(BeNil())
		Expect(p2).NotTo(BeNil())
		Expect(pm.LoadValue(p2.Endpoint)).To(Equal("http://localhost/hello"))

		_, err = pm.FindByName("test-jwt-4")
		Expect(err).To(Equal(ErrLoginProviderNotFound))
	})
	It("should be able to delete a config", func() {
		p := createConfig("test-jwt-5")
		err := pm.AddProvider(p)
		Expect(err).To(BeNil())

		p2, err := pm.FindByName("test-jwt-5")
		Expect(err).To(BeNil())
		Expect(p2).NotTo(BeNil())

		err = pm.DeleteProvider("test-jwt-5")
		Expect(err).To(BeNil())

		p3, err := pm.FindByName("test-jwt-5")
		Expect(err).To(Equal(ErrLoginProviderNotFound))
		Expect(p3).To(BeZero())
	})
	It("should list all configs", func() {
		p := createConfig("test-jwt-6")
		_ = pm.AddProvider(p)
		p2 := createConfig("test-jwt-7")
		_ = pm.AddProvider(p2)

		items, err := pm.ListProviders()
		Expect(err).To(BeNil())
		Expect(len(items)).To(Equal(2))
	})
})

func createConfig(name string) ProviderConfig {
	return ProviderConfig{
		Name: name,
		Type: "bearer",
		ClientID: &ValueReader{
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
		Endpoint: &ValueReader{
			Type:  "text",
			Value: "http://localhost",
		},
	}
}

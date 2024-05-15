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

package datahub_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/mimiro-io/datahub/internal/conf"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub"
	"github.com/mimiro-io/datahub/internal/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("The dataset endpoint", Ordered, Serial, func() {
	var app *datahub.DatahubInstance
	var mockLayer *MockLayer

	location := "./http_integration_test"
	queryURL := "http://localhost:24997/query"
	dsURL := "http://localhost:24997/datasets/bananas"
	proxyDsURL := "http://localhost:24997/datasets/cucumbers"
	virtualDsURL := "http://localhost:24997/datasets/virtual"

	datasetsURL := "http://localhost:24997/datasets"
	BeforeAll(func() {
		_ = os.RemoveAll(location)
		_ = os.Setenv("STORE_LOCATION", location)
		_ = os.Setenv("PROFILE", "test")
		_ = os.Setenv("SERVER_PORT", "24997")
		_ = os.Setenv("FULLSYNC_LEASE_TIMEOUT", "500ms")

		oldOut := os.Stdout
		oldErr := os.Stderr
		devNull, _ := os.Open("/dev/null")
		os.Stdout = devNull
		os.Stderr = devNull
		// app, _ = datahub.Start(context.Background())
		config, _ := conf.LoadConfig("")
		app, _ = datahub.NewDatahubInstance(config)
		go app.Start()

		mockLayer = NewMockLayer()
		go func() {
			_ = mockLayer.echo.Start(":7778")
		}()
		os.Stdout = oldOut
		os.Stderr = oldErr
	})
	AfterAll(func() {
		_ = mockLayer.echo.Shutdown(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		err := app.Stop(ctx)
		defer cancel()
		Expect(err).To(BeNil())
		err = os.RemoveAll(location)
		Expect(err).To(BeNil())
		_ = os.Unsetenv("STORE_LOCATION")
		_ = os.Unsetenv("PROFILE")
		_ = os.Unsetenv("SERVER_PORT")
		_ = os.Unsetenv("FULLSYNC_LEASE_TIMEOUT")
	})

	Describe("The dataset root API", Ordered, func() {
		It("Should create a regular dataset", func() {
			// create new dataset
			res, err := http.Post(dsURL, "application/json", strings.NewReader(""))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
		})
		It("Should retrieve a regular dataset", func() {
			res, err := http.Get(dsURL)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			m := map[string]interface{}{}
			_ = json.Unmarshal(b, &m)
			Expect(m["id"]).To(Equal("ns0:bananas"))
			refs := m["refs"].(map[string]interface{})
			Expect(refs["ns2:type"]).To(Equal("ns1:dataset"))
		})
		It("Should create a proxy dataset", func() {
			// create new dataset
			/*
				type createDatasetConfig struct {
					ProxyDatasetConfig *proxyDatasetConfig `json:"proxyDatasetConfig"`
					PublicNamespaces   []string            `json:"publicNamespaces"`
				}

				type proxyDatasetConfig struct {
					AuthProvider        string `json:"authProvider"`
					RemoteUrl           string `json:"remoteUrl"`
					UpstreamTransform   string `json:"upstreamTransform"`
					DownstreamTransform string `json:"downstreamTransform"`
				}
			*/
			res, err := http.Post(proxyDsURL+"?proxy=true", "application/json", strings.NewReader(
				`{"proxyDatasetConfig": {"remoteUrl": "http://localhost:7778/datasets/tomatoes"}}`))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
		})
		It("Should reject a proxy dataset if misconfigured", func() {
			res, err := http.Post(proxyDsURL+"2?proxy=true", "application/json", strings.NewReader(
				`{"proxyDatasetConfig": {"remoteUrl": ""}}`))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(400))
			b, _ := io.ReadAll(res.Body)
			Expect(string(b)).To(Equal("{\"message\":\"invalid proxy configuration provided\"}\n"))
		})
		It("Should retrieve a proxy dataset", func() {
			res, err := http.Get(proxyDsURL)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			m := map[string]interface{}{}
			_ = json.Unmarshal(b, &m)
			Expect(m["id"]).To(Equal("ns0:cucumbers"))
			refs := m["refs"].(map[string]interface{})
			Expect(refs["ns2:type"]).To(Equal("ns1:proxy-dataset"))
		})
		It("Should list both regular and proxy datasets", func() {
			res, err := http.Get(datasetsURL)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			var l []map[string]interface{}
			_ = json.Unmarshal(b, &l)
			Expect(len(l)).To(Equal(3), "core.Dataset, bananas, cucumbers are listed")
		})

		It("Should create a virtual dataset with base64 encoded transform", func() {
			// create new dataset
			js := `function build_entities(params = {}, since = "", limit = -1) {
						const prefix1 = AssertNamespacePrefix("http://data.mimiro.io/things/");
						const prefix2 = AssertNamespacePrefix("http://data.mimiro.io/items/");
						for (let i = 0; i < 3; i++) {
							const e = NewEntity();
							SetId(e, prefix1+":virtual-" + i);
							SetProperty(e, prefix1, "virtual", i);
							SetProperty(e, prefix1, "params", params.param);
							AddReference(e, prefix1, "type", prefix2+":virtual-entity");
							if (isNaN(parseInt(since)) || i > parseInt(since)) {
								Emit(e);
							}
						}
						const newSince=5;
						return newSince;
					}`
			f := base64.StdEncoding.EncodeToString([]byte(js))
			res, err := http.Post(virtualDsURL, "application/json", strings.NewReader(fmt.Sprintf(`{
				"virtualDatasetConfig": {
					"transform": "%s"
				}
			}`, f)))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			res, err = http.Get(virtualDsURL)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			m := map[string]interface{}{}
			_ = json.Unmarshal(b, &m)
			Expect(m["id"]).To(Equal("ns0:virtual"))
			refs := m["refs"].(map[string]interface{})
			Expect(refs["ns2:type"]).To(Equal("ns1:virtual-dataset"))
		})
	})
	Describe("The /entities and /changes API support JSON-LD", Ordered, func() {
		It("Should expose changes as JSON-LD", func() {
			// populate dataset
			payload := strings.NewReader(bananasFromTo(1, 10, false))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)

			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// read it back
			client := &http.Client{}
			req, err := http.NewRequest("GET", dsURL+"/changes", nil)
			req.Header.Add("Accept", "application/ld+json")
			res, err = client.Do(req)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			bodyBytes, err := io.ReadAll(res.Body)
			Expect(err).To(BeNil())

			var jsonLd []interface{}
			err = json.Unmarshal(bodyBytes, &jsonLd)
			Expect(err).To(BeNil())

			// check that the JSON-LD context is present
			Expect(jsonLd[0].(map[string]interface{})["@context"]).NotTo(BeZero())

			Expect(jsonLd[11].(map[string]interface{})["rdf:type"]).NotTo(BeZero())
		})
	})
	Describe("The /entities and /changes API endpoints for regular datasets", Ordered, func() {
		It("Should accept a single batch of changes", func() {
			// populate dataset
			payload := strings.NewReader(bananasFromTo(1, 10, false))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)

			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// read it back
			res, err = http.Get(dsURL + "/changes")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			bodyBytes, err := io.ReadAll(res.Body)
			Expect(err).To(BeNil())
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(12), "expected 10 entities plus @context and @continuation")
		})

		It("Should accept multiple overlapping batches of changes", func() {
			// replace 5-10 and add 11-15
			payload := strings.NewReader(bananasFromTo(5, 15, false))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// replace 10-15 and add 16-20
			payload = strings.NewReader(bananasFromTo(10, 20, false))
			res, err = http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// read it back
			res, err = http.Get(dsURL + "/changes")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(22), "expected 20 entities plus @context and @continuation")
		})

		It("Should record deleted states", func() {
			payload := strings.NewReader(bananasFromTo(7, 8, true))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// read changes back
			res, err = http.Get(dsURL + "/changes")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).
				To(Equal(24), "expected 20 entities plus 2 deleted-changes plus @context and @continuation")
			Expect(entities[7].IsDeleted).To(BeFalse(), "original change 7 is still undeleted")
			Expect(entities[22].IsDeleted).To(BeTrue(), "deleted state for 7  is a new change at end of list")

			// read entities back
			res, err = http.Get(dsURL + "/entities")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ = io.ReadAll(res.Body)
			_ = res.Body.Close()
			entities = nil
			err = json.Unmarshal(bodyBytes, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(22), "expected 20 entities plus @context and @continuation")
			Expect(entities[7].IsDeleted).To(BeTrue(), "entity 7 is deleted")
		})

		It("Should do deletion detection in a fullsync", func() {
			// only send IDs 4 through 16 in batches as fullsync
			// 1-3 and 17-20 should end up deleted

			// first batch with "start" header
			payload := strings.NewReader(bananasFromTo(4, 8, false))
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			res, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			cancel()

			// 2nd batch
			payload = strings.NewReader(bananasFromTo(9, 12, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "42")
			res, err = http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			cancel()

			// last batch with "end" signal
			payload = strings.NewReader(bananasFromTo(13, 16, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "42")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			res, err = http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			cancel()

			// read changes back
			res, err = http.Get(dsURL + "/changes")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(33), "expected 20 entities plus 11 changes and @context and @continuation")
			Expect(entities[7].IsDeleted).To(BeFalse(), "original change 7 is still undeleted")
			Expect(entities[21].IsDeleted).To(BeTrue(), "deleted state for 7  is a new change at end of list")

			// read entities back
			res, err = http.Get(dsURL + "/entities")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ = io.ReadAll(res.Body)
			_ = res.Body.Close()
			entities = nil
			err = json.Unmarshal(bodyBytes, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(22), "expected 20 entities plus @context and @continuation")
			// remove context
			entities = entities[1:]
			for i := 0; i < 3; i++ {
				Expect(entities[i].IsDeleted).To(BeTrue(), "entity was not part of fullsync, should be deleted: ", i)
			}
			for i := 3; i < 16; i++ {
				Expect(entities[i].IsDeleted).To(BeFalse(), "entity was part of fullsync, should be active: ", i)
			}
			for i := 16; i < 20; i++ {
				Expect(entities[i].IsDeleted).To(BeTrue(), "entity was not part of fullsync, should be deleted: ", i)
			}
		})

		It("should keep fullsync requests with same sync-id in parallel", func() {
			// only send IDs 4 through 16 in batches as fullsync
			// 1-3 and 17-20 should end up deleted

			// first batch with "start" header
			payload := strings.NewReader(bananasFromTo(4, 4, false))
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "43")
			_, _ = http.DefaultClient.Do(req)
			cancel()

			// next, updated id 5 with wrong sync-id. should not be registered as "seen" and therefore be deleted after fs
			payload = strings.NewReader(bananasFromTo(5, 5, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "44")
			res, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			cancel()
			Expect(res.StatusCode).To(Equal(409), "request should be rejected because fullsync is going on")

			// also try to add id 5 without sync-id. should still be rejected
			payload = strings.NewReader(bananasFromTo(5, 5, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			res, err = http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			cancel()
			Expect(res.StatusCode).To(Equal(409), "request should be rejected because fullsync is going on")

			// 10 batches in parallel with correct sync-id
			wg := sync.WaitGroup{}
			for i := 6; i < 16; i++ {
				wg.Add(1)
				id := i
				go func() {
					payloadLocal := strings.NewReader(bananasFromTo(id, id, false))
					ctxLocal, cancelLocal := context.WithTimeout(context.Background(), 1000*time.Millisecond)
					reqLocal, _ := http.NewRequestWithContext(ctxLocal, "POST", dsURL+"/entities", payloadLocal)
					reqLocal.Header.Add("universal-data-api-full-sync-id", "43")
					_, _ = http.DefaultClient.Do(reqLocal)
					cancelLocal()
					wg.Done()
				}()
			}

			wg.Wait()

			// last batch with "end" signal
			payload = strings.NewReader(bananasFromTo(16, 16, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "43")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			_, _ = http.DefaultClient.Do(req)
			cancel()

			// read changes back
			res, err = http.Get(dsURL + "/changes")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).
				To(Equal(34), "expected 31 changes from before plus deletion of id5 and @context and @continuation")
			Expect(entities[32].IsDeleted).To(BeTrue(), "deleted state for 5  is a new change at end of list")
		})
		It("should abandon fullsync when new fullsync is started", func() {
			// start a fullsync
			payload := strings.NewReader(bananasFromTo(1, 1, false))
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "45")
			res, err := http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// start another fullsync
			payload = strings.NewReader(bananasFromTo(1, 1, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "46")
			res, err = http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// try to append to first fullsync, should be rejected
			payload = strings.NewReader(bananasFromTo(2, 2, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "45")
			res, err = http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(409), "expect rejection since syncid 45 is not active anymore")

			// complete second sync
			payload = strings.NewReader(bananasFromTo(16, 16, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "46")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			res, err = http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200), "sync 46 accept requests")
		})
		It("should abandon fullsync after a timeout period without new requests", func() {
			// start a fullsync
			payload := strings.NewReader(bananasFromTo(1, 1, false))
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "47")
			res, err := http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// exceed fullsync timeout
			time.Sleep(501 * time.Millisecond)

			// send next fullsync batch. should be OK even though lease is timed out
			payload = strings.NewReader(bananasFromTo(2, 2, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "47")
			res, err = http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// send next end signal. should produce error since lease should have timed out
			payload = strings.NewReader(bananasFromTo(3, 3, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-end", "true")
			req.Header.Add("universal-data-api-full-sync-id", "47")
			res, err = http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(410))
		})

		It("Should pageinate over entities with continuation token", func() {
			payload := strings.NewReader(bananasFromTo(1, 100, false))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// read first page of 10 entities back
			res, err = http.Get(dsURL + "/entities?limit=10")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			var m []map[string]interface{}
			_ = json.Unmarshal(bodyBytes, &m)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(12), "expected 10 entities plus @context and @continuation")
			Expect(entities[1].ID).To(Equal("ns3:1"))
			token := m[11]["token"].(string)

			// read next page
			res, err = http.Get(dsURL + "/entities?limit=90&from=" + url.QueryEscape(token))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ = io.ReadAll(res.Body)
			_ = res.Body.Close()
			err = json.Unmarshal(bodyBytes, &entities)
			_ = json.Unmarshal(bodyBytes, &m)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(92), "expected 90 entities plus @context and @continuation")
			Expect(entities[1].ID).To(Equal("ns3:11"))
			token = m[91]["token"].(string)

			// read next page after all consumed
			res, err = http.Get(dsURL + "/entities?limit=10&from=" + url.QueryEscape(token))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			bodyBytes, _ = io.ReadAll(res.Body)
			_ = res.Body.Close()
			err = json.Unmarshal(bodyBytes, &entities)
			_ = json.Unmarshal(bodyBytes, &m)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(2), "expected 0 entities plus @context and @continuation")
			Expect(entities[1].ID).To(Equal("@continuation"))
		})
	})
	Describe("The /changes and /entities endpoints for proxy datasets", Ordered, func() {
		It("Should fetch from remote for GET /changes without token", func() {
			// TODO: why do we need to GET twice? the first GET does not get the comlete list of namespaces
			http.Get(proxyDsURL + "/changes")
			res, err := http.Get(proxyDsURL + "/changes")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			var entities []*server.Entity
			err = json.Unmarshal(b, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(12), "context, 10 entities and continuation")
			Expect(entities[1].ID).To(Equal("ns4:c-0"), "first page id range starts with 0")
			Expect(mockLayer.RecordedURI).To(Equal("/datasets/tomatoes/changes"))
			var m []map[string]interface{}
			_ = json.Unmarshal(b, &m)
			Expect(m[11]["token"]).To(Equal("nextplease"))
			Expect(m[0]["namespaces"]).To(Equal(map[string]interface{}{
				"ns0": "http://data.mimiro.io/core/dataset/",
				"ns1": "http://data.mimiro.io/core/",
				"ns2": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
				"ns3": "http://example.com",
				"ns4": "http://example.mimiro.io/",
			}))
		})
		It("Should fetch from remote for GET /changes with token and limit", func() {
			res, err := http.Get(proxyDsURL + "/changes?since=theweekend&limit=3")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			var entities []*server.Entity
			err = json.Unmarshal(b, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(5), "context, 3 entities and continuation")
			Expect(entities[1].ID).To(Equal("ns4:c-100"), "later page mock id range starts with 100")
			Expect(mockLayer.RecordedURI).To(Equal("/datasets/tomatoes/changes?limit=3&since=theweekend"))
			var m []map[string]interface{}
			_ = json.Unmarshal(b, &m)
			Expect(m[4]["token"]).To(Equal("lastpage"))
			Expect(m[0]["namespaces"]).To(Equal(map[string]interface{}{
				"ns0": "http://data.mimiro.io/core/dataset/",
				"ns1": "http://data.mimiro.io/core/",
				"ns2": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
				"ns3": "http://example.com",
				"ns4": "http://example.mimiro.io/",
			}))
		})
		It("Should fetch from remote for GET /entities", func() {
			res, err := http.Get(proxyDsURL + "/entities?from=theweekend&limit=3")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			var entities []*server.Entity
			err = json.Unmarshal(b, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).
				To(Equal(11), "context, 10 entities (remote ignored limit) and no continuation (none returned from remote)")
			Expect(entities[1].ID).To(Equal("ns4:e-0"))
			Expect(mockLayer.RecordedURI).To(Equal("/datasets/tomatoes/entities?from=theweekend&limit=3"))
			var m []map[string]interface{}
			_ = json.Unmarshal(b, &m)
			Expect(m[0]["namespaces"]).To(Equal(map[string]interface{}{
				"ns0": "http://data.mimiro.io/core/dataset/",
				"ns1": "http://data.mimiro.io/core/",
				"ns2": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
				"ns3": "http://example.com",
				"ns4": "http://example.mimiro.io/",
			}))
		})
		It("Should push to remote for POST /entities", func() {
			payload := strings.NewReader(bananasFromTo(1, 3, false))
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", proxyDsURL+"/entities", payload)
			res, err := http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			recorded := mockLayer.RecordedEntities["tomatoes"]
			Expect(len(recorded)).To(Equal(4), "context and 3 entities")
			Expect(recorded[0].ID).To(Equal("@context"))
			Expect(recorded[1].ID).To(Equal("1"))
			Expect(recorded[2].ID).To(Equal("2"))
			Expect(recorded[3].ID).To(Equal("3"))
			var m []map[string]interface{}
			_ = json.Unmarshal(mockLayer.RecordedBytes["tomatoes"], &m)
			Expect(m[0]["namespaces"]).To(Equal(map[string]interface{}{"_": "http://example.com"}))
		})
		It("Should forward fullsync headers for POST /entities", func() {
			payload := strings.NewReader(bananasFromTo(1, 17, false))
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", proxyDsURL+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "46")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			res, err := http.DefaultClient.Do(req)
			cancel()
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			recorded := mockLayer.RecordedEntities["tomatoes"]
			Expect(mockLayer.RecordedHeaders.Get("universal-data-api-full-sync-start")).To(Equal("true"))
			Expect(mockLayer.RecordedHeaders.Get("universal-data-api-full-sync-id")).To(Equal("46"))
			Expect(mockLayer.RecordedHeaders.Get("universal-data-api-full-sync-end")).To(Equal("true"))
			Expect(len(recorded)).To(Equal(18), "context and 17 entities")
		})
		It("Should expose publicNamespaces if configured on proxy dataset", func() {
			// delete proxy ds
			req, _ := http.NewRequest("DELETE", proxyDsURL, nil)
			res, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// make sure it's gone
			res, err = http.Get(proxyDsURL)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(404))

			// recreate with publicNamespaces
			res, err = http.Post(proxyDsURL+"?proxy=true", "application/json", strings.NewReader(
				`{
							"proxyDatasetConfig": {
								"remoteUrl": "http://localhost:7778/datasets/tomatoes",
                            	"authProviderName": "local"
                         	},
							"publicNamespaces": ["http://example.com", "http://example.mimiro.io/"]
						}`))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// read it back, hopefully with with publicNamespaces applied
			res, err = http.Get(proxyDsURL + "/changes?limit=1")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			var entities []*server.Entity
			err = json.Unmarshal(b, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(3), "context, 1 entity and continuation")
			Expect(entities[1].ID).To(Equal("ns4:c-0"))
			Expect(mockLayer.RecordedURI).To(Equal("/datasets/tomatoes/changes?limit=1"))
			var m []map[string]interface{}
			_ = json.Unmarshal(b, &m)
			Expect(m[0]["namespaces"]).To(Equal(map[string]interface{}{
				"ns3": "http://example.com",
				"ns4": "http://example.mimiro.io/",
			}))
			Expect(mockLayer.RecordedHeaders.Get("Authorization")).To(BeZero(),
				"there is no authProvider, fallback to unauthed")
		})

		It("Should apply authProvider if configured", func() {
			payload := strings.NewReader(`{
					"name": "local",
					"type": "basic",
					"user": { "value": "u0", "type":"text" },
					"password": { "value":"u1","type":"text"}
				}`)
			req, _ := http.NewRequest("POST", "http://localhost:24997/provider/logins", payload)
			req.Header = http.Header{
				"Content-Type": []string{"application/json"},
			}
			res, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			// read changes and verify auth is applied
			res, err = http.Get(proxyDsURL + "/changes?limit=1")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			b, _ := io.ReadAll(res.Body)
			var entities []*server.Entity
			err = json.Unmarshal(b, &entities)
			Expect(err).To(BeNil())
			Expect(len(entities)).To(Equal(3), "context, 1 entity and continuation")
			Expect(entities[1].ID).To(Equal("ns4:c-0"))
			Expect(mockLayer.RecordedURI).To(Equal("/datasets/tomatoes/changes?limit=1"))
			Expect(mockLayer.RecordedHeaders.Get("Authorization")).To(Equal("Basic dTA6dTE="),
				"basic auth header expected")
		})
	})
	Describe("the /query endpoint", Ordered, func() {
		It("can find changes", func() {
			// relying on dataset being populated from previous cases
			// do query
			js := `
					function do_query() {
						const changes = GetDatasetChanges("bananas")
						let obj = { "bananaCount": changes.Entities.length };
						WriteQueryResult(obj);
					}
					`
			queryEncoded := base64.StdEncoding.EncodeToString([]byte(js))

			query := map[string]any{"query": queryEncoded}
			queryBytes, _ := json.Marshal(query)

			res, err := http.Post(queryURL, "application/x-javascript-query", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ := io.ReadAll(res.Body)
			Expect(string(body)).To(Equal("[{\"bananaCount\":100}]"))
		})
		It("can find single ids", func() {
			query := map[string]any{"entityId": "ns3:16"}
			queryBytes, _ := json.Marshal(query)
			res, err := http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ := io.ReadAll(res.Body)
			var rMap []map[string]any
			err = json.Unmarshal(body, &rMap)
			Expect(err).To(BeNil())
			Expect(rMap).NotTo(BeZero())
			Expect(rMap[1]["id"]).To(Equal("ns3:16"))
			Expect(rMap[1]["recorded"]).NotTo(BeZero())
		})
		It("can find outgoing relations from startUri and see dataset", func() {
			payload := strings.NewReader(bananaRelations(
				bananaRel{fromBanana: 1, toBananas: []int{2, 3}},
				bananaRel{fromBanana: 2, toBananas: []int{3, 4, 5, 6, 7}},
			))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			query := map[string]any{"startingEntities": []string{"ns3:2"}, "predicate": "*", "noPartialMerging": true}
			queryBytes, _ := json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ := io.ReadAll(res.Body)
			var rArr []any
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result := rArr[1].([]any)
			Expect(len(result)).To(Equal(5))
			//fmt.Println(string(body))
			Expect(result[4].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:3"))
			Expect(result[4].([]any)[2].(map[string]any)["props"].(map[string]any)["http://data.mimiro.io/core/partials"]).To(HaveLen(1))
			Expect(result[4].([]any)[2].(map[string]any)["props"].(map[string]any)["http://data.mimiro.io/core/partials"]).To(HaveLen(1))
			Expect(result[4].([]any)[2].(map[string]any)["props"].(map[string]any)["http://data.mimiro.io/core/partials"].([]any)[0].(map[string]any)["props"].(map[string]any)["http://data.mimiro.io/core/datasetname"]).To(Equal("bananas"))

			Expect(result[3].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:4"))
			Expect(result[2].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:5"))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:6"))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:7"))
		})

		It("can find outgoing relations from startUri", func() {
			payload := strings.NewReader(bananaRelations(
				bananaRel{fromBanana: 1, toBananas: []int{2, 3}},
				bananaRel{fromBanana: 2, toBananas: []int{3, 4, 5, 6, 7}},
			))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			query := map[string]any{"startingEntities": []string{"ns3:2"}, "predicate": "*"}
			queryBytes, _ := json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ := io.ReadAll(res.Body)
			var rArr []any
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result := rArr[1].([]any)
			Expect(len(result)).To(Equal(5))
			Expect(result[4].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:3"))
			Expect(result[3].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:4"))
			Expect(result[2].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:5"))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:6"))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:7"))
		})
		It("can page through queried outgoing relations", func() {
			payload := strings.NewReader(bananaRelations(
				bananaRel{fromBanana: 1, toBananas: []int{2, 3}},
				bananaRel{fromBanana: 2, toBananas: []int{3, 4, 5, 6, 7}},
			))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			query := map[string]any{"startingEntities": []string{"ns3:2"}, "predicate": "*", "limit": 2}
			queryBytes, _ := json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ := io.ReadAll(res.Body)
			var rArr []any
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result := rArr[1].([]any)
			Expect(len(result)).To(Equal(2))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:6"))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:7"))
			cont := rArr[2]
			query = map[string]any{"continuations": cont, "limit": 2}
			queryBytes, _ = json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ = io.ReadAll(res.Body)
			rArr = []any{}
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result = rArr[1].([]any)
			Expect(len(result)).To(Equal(2))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:4"))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:5"))

			cont = rArr[2]
			query = map[string]any{"continuations": cont, "limit": 2}
			queryBytes, _ = json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ = io.ReadAll(res.Body)
			rArr = []any{}
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result = rArr[1].([]any)
			Expect(len(result)).To(Equal(1))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:3"))
			cont = rArr[2]
			Expect(len(cont.([]any))).To(Equal(0))
		})
		It("can find inverse relations from startUri", func() {
			payload := strings.NewReader(bananaRelations(
				bananaRel{fromBanana: 1, toBananas: []int{2, 3}},
				bananaRel{fromBanana: 2, toBananas: []int{3, 4}},
				bananaRel{fromBanana: 4, toBananas: []int{3, 2, 1}},
			))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			query := map[string]any{"startingEntities": []string{"ns3:3"}, "predicate": "*", "inverse": true}
			queryBytes, _ := json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ := io.ReadAll(res.Body)
			var rArr []any
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result := rArr[1].([]any)
			Expect(len(result)).To(Equal(3))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:1"))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:2"))
			Expect(result[2].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:4"))
		})

		It("can page through queried inverse relations", func() {
			payload := strings.NewReader(bananaRelations(
				bananaRel{fromBanana: 1, toBananas: []int{2, 3}},
				bananaRel{fromBanana: 2, toBananas: []int{3, 4}},
				bananaRel{fromBanana: 3, toBananas: []int{2, 1}},
				bananaRel{fromBanana: 4, toBananas: []int{3, 2, 1}},
				bananaRel{fromBanana: 5, toBananas: []int{3, 2, 1}},
				bananaRel{fromBanana: 6, toBananas: []int{3, 2, 1}},
				bananaRel{fromBanana: 7, toBananas: []int{3, 2, 1}},
			))
			res, err := http.Post(dsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))

			query := map[string]any{
				"startingEntities": []string{"ns3:3"},
				"predicate":        "*",
				"inverse":          true,
				"limit":            2,
			}
			queryBytes, _ := json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ := io.ReadAll(res.Body)
			var rArr []any
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result := rArr[1].([]any)
			Expect(len(result)).To(Equal(2))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:1"))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:2"))

			cont := rArr[2]
			savedCont := cont
			query = map[string]any{"continuations": cont, "limit": 2}
			queryBytes, _ = json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ = io.ReadAll(res.Body)
			rArr = []any{}
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result = rArr[1].([]any)
			Expect(len(result)).To(Equal(2))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:4"))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:5"))

			cont = rArr[2]
			query = map[string]any{"continuations": cont, "limit": 2}
			queryBytes, _ = json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ = io.ReadAll(res.Body)
			rArr = []any{}
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result = rArr[1].([]any)
			Expect(len(result)).To(Equal(2))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:6"))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:7"))

			// go through last pages with different batch sizes
			query = map[string]any{"continuations": savedCont, "limit": 3}
			queryBytes, _ = json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ = io.ReadAll(res.Body)
			rArr = []any{}
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result = rArr[1].([]any)
			Expect(len(result)).To(Equal(3))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:4"))
			Expect(result[1].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:5"))
			Expect(result[2].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:6"))

			cont = rArr[2]
			query = map[string]any{"continuations": cont, "limit": 2}
			queryBytes, _ = json.Marshal(query)
			res, err = http.Post(queryURL, "application/javascript", bytes.NewReader(queryBytes))
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(200))
			body, _ = io.ReadAll(res.Body)
			rArr = []any{}
			err = json.Unmarshal(body, &rArr)
			Expect(err).To(BeNil())
			Expect(rArr).NotTo(BeZero())
			result = rArr[1].([]any)
			Expect(len(result)).To(Equal(1))
			Expect(result[0].([]any)[2].(map[string]any)["id"]).To(Equal("ns3:7"))
		})
	})
	Describe("the /changes and /entities endpoints for virtual datasets", Ordered, func() {
		It("Should reject POST /entities for virtual datasets", func() {
			payload := strings.NewReader(bananasFromTo(1, 3, false))
			res, err := http.Post(virtualDsURL+"/entities", "application/json", payload)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(http.StatusNotImplemented))
		})
		It("Should reject GET /entities for virtual datasets", func() {
			res, err := http.Get(virtualDsURL + "/entities")
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeZero())
			Expect(res.StatusCode).To(Equal(http.StatusNotImplemented))
		})
		Describe("Should execute transform on GET /changes for virtual datasets", Ordered, func() {
			It("without since", func() {
				res, err := http.Get(virtualDsURL + "/changes")
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeZero())
				Expect(res.StatusCode).To(Equal(http.StatusOK))

				nsm := egdm.NewNamespaceContext()
				p := egdm.NewEntityParser(nsm).WithLenientNamespaceChecks()
				ec, err := p.LoadEntityCollection(res.Body)
				Expect(err).To(BeNil())

				Expect(ec.Entities).NotTo(BeZero())
				Expect(ec.NamespaceManager).NotTo(BeZero())
				Expect(ec.Continuation).NotTo(BeZero())
				Expect(len(ec.Entities)).To(Equal(3))
				Expect(ec.Entities[0].ID).To(Equal("ns5:virtual-0"))
				Expect(ec.Entities[1].ID).To(Equal("ns5:virtual-1"))
				Expect(ec.Entities[2].ID).To(Equal("ns5:virtual-2"))
				Expect(ec.Continuation.Token).To(Equal("5"))
			})
			It("with since", func() {
				res, err := http.Get(virtualDsURL + "/changes?since=1")
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeZero())
				Expect(res.StatusCode).To(Equal(http.StatusOK))

				nsm := egdm.NewNamespaceContext()
				p := egdm.NewEntityParser(nsm).WithLenientNamespaceChecks()
				ec, err := p.LoadEntityCollection(res.Body)
				Expect(err).To(BeNil())

				Expect(ec.Entities).NotTo(BeZero())
				Expect(ec.NamespaceManager).NotTo(BeZero())
				Expect(ec.NamespaceManager.GetNamespaceMappings()).To(Equal(map[string]string{
					"ns0": "http://data.mimiro.io/core/dataset/",
					"ns1": "http://data.mimiro.io/core/",
					"ns2": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
					"ns3": "http://example.com",
					"ns4": "http://example.mimiro.io/",
					"ns5": "http://data.mimiro.io/things/",
					"ns6": "http://data.mimiro.io/items/",
				}))
				Expect(ec.Continuation).NotTo(BeZero())
				Expect(len(ec.Entities)).To(Equal(1))
				Expect(ec.Entities[0].ID).To(Equal("ns5:virtual-2"))
				Expect(ec.Continuation.Token).To(Equal("5"))
			})
			It("with public namespaces", func() {
				// get transform from virtual dataset
				res, err := http.Get(virtualDsURL)
				Expect(err).To(BeNil())
				j := map[string]any{}
				err = json.NewDecoder(res.Body).Decode(&j)
				Expect(err).To(BeNil())
				t := j["props"].(map[string]any)["ns0:transform"].(string)

				// delete it
				req, _ := http.NewRequest("DELETE", virtualDsURL, nil)
				res, err = http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeZero())
				Expect(res.StatusCode).To(Equal(200))

				// recreate with transform and public namespaces
				res, err = http.Post(virtualDsURL, "application/json", strings.NewReader(fmt.Sprintf(`{
					"virtualDatasetConfig": {
						"transform": "%s"
					},
					"publicNamespaces": ["http://data.mimiro.io/things/", "http://data.mimiro.io/items/"]
				}`, t)))
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeZero())
				Expect(res.StatusCode).To(Equal(200))

				res, err = http.Get(virtualDsURL + "/changes?since=1")
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeZero())
				Expect(res.StatusCode).To(Equal(http.StatusOK))

				ec, err := egdm.NewEntityParser(egdm.NewNamespaceContext()).
					WithLenientNamespaceChecks().
					LoadEntityCollection(res.Body)
				Expect(err).To(BeNil())

				Expect(ec.Entities).NotTo(BeZero())
				Expect(ec.NamespaceManager).NotTo(BeZero())
				Expect(ec.NamespaceManager.GetNamespaceMappings()).To(Equal(map[string]string{
					"ns5": "http://data.mimiro.io/things/",
					"ns6": "http://data.mimiro.io/items/",
				}))
				Expect(ec.Continuation).NotTo(BeZero())
				Expect(len(ec.Entities)).To(Equal(1))
				Expect(ec.Entities[0].ID).To(Equal("ns5:virtual-2"))
				Expect(ec.Continuation.Token).To(Equal("5"))
			})
			It("with request body array", func() {
				req, err := http.NewRequest("GET", virtualDsURL+"/changes", strings.NewReader(`["12345", "67890"]`))
				Expect(err).To(BeNil())
				res, err := http.DefaultClient.Do(req)
				Expect(err).To(BeNil())
				Expect(res).NotTo(BeZero())
				Expect(res.StatusCode).To(Equal(200))
				ec, err := egdm.NewEntityParser(egdm.NewNamespaceContext()).
					WithLenientNamespaceChecks().
					LoadEntityCollection(res.Body)
				Expect(err).To(BeNil())

				Expect(ec.Entities).NotTo(BeZero())
				Expect(ec.Entities[0].Properties).To(HaveKeyWithValue("ns5:params", []any{"12345", "67890"}))
			})
		})
	})
})

func bananaRelations(rels ...bananaRel) string {
	prefix := `[ { "id" : "@context", "namespaces" : { "_" : "http://example.com" } }, `

	var bananas []string

	for _, rel := range rels {
		refStr := ""
		for i, rStr := range rel.toBananas {
			refStr = refStr + fmt.Sprintf(`"%v"`, rStr)
			if i < len(rel.toBananas)-1 {
				refStr = refStr + ","
			}
		}
		bananas = append(bananas, fmt.Sprintf(`{ "id" : "%v", "refs": {"link": [%v]} }`, rel.fromBanana, refStr))
	}

	return prefix + strings.Join(bananas, ",") + "]"
}

func bananasFromTo(from, to int, deleted bool) string {
	prefix := `[ { "id" : "@context", "namespaces" : { "_" : "http://example.com" } }, `

	var bananas []string

	for i := from; i <= to; i++ {
		if deleted {
			bananas = append(bananas, fmt.Sprintf(`{ "id" : "%v", "deleted": true }`, i))
		} else {
			bananas = append(bananas, fmt.Sprintf(`{ "id" : "%v" }`, i))
		}
	}

	return prefix + strings.Join(bananas, ",") + "]"
}

type bananaRel struct {
	fromBanana int
	toBananas  []int
}

type MockLayer struct {
	RecordedEntities map[string][]*server.Entity
	echo             *echo.Echo
	RecordedURI      string
	RecordedBytes    map[string][]byte
	RecordedHeaders  http.Header
}

type Continuation struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

func NewMockLayer() *MockLayer {
	e := echo.New()
	result := &MockLayer{}
	result.RecordedEntities = make(map[string][]*server.Entity)
	result.RecordedBytes = make(map[string][]byte)
	result.echo = e
	e.HideBanner = true

	ctx := make(map[string]interface{})
	ctx["id"] = "@context"
	ns := make(map[string]string)
	ns["ex"] = "http://example.mimiro.io/"
	ns["_"] = "http://default.mimiro.io/"
	ctx["namespaces"] = ns

	e.POST("/datasets/tomatoes/entities", func(context echo.Context) error {
		b, _ := io.ReadAll(context.Request().Body)
		entities := []*server.Entity{}
		_ = json.Unmarshal(b, &entities)
		result.RecordedEntities["tomatoes"] = entities
		result.RecordedBytes["tomatoes"] = b
		result.RecordedHeaders = context.Request().Header
		return context.NoContent(http.StatusOK)
	})
	e.GET("/datasets/tomatoes/entities", func(context echo.Context) error {
		r := make([]interface{}, 0)
		r = append(r, ctx)
		result.RecordedURI = context.Request().RequestURI
		result.RecordedHeaders = context.Request().Header

		// add some objects
		for i := 0; i < 10; i++ {
			e := server.NewEntity("ex:e-"+strconv.Itoa(i), 0)
			r = append(r, e)
		}
		return context.JSON(http.StatusOK, r)
	})

	e.GET("/datasets/tomatoes/changes", func(context echo.Context) error {
		r := make([]interface{}, 0)
		r = append(r, ctx)
		result.RecordedURI = context.Request().RequestURI
		result.RecordedHeaders = context.Request().Header

		// check for since
		since := context.QueryParam("since")
		l := context.QueryParam("limit")
		cnt := 10
		if limit, ok := strconv.Atoi(l); ok == nil {
			cnt = limit
		}
		switch since {
		case "":
			// add some objects
			for i := 0; i < cnt; i++ {
				e := server.NewEntity("ex:c-"+strconv.Itoa(i), 0)
				r = append(r, e)
			}
			c := &Continuation{ID: "@continuation", Token: "nextplease"}
			r = append(r, c)
		case "lastpage":
			c := &Continuation{ID: "@continuation", Token: "lastpage"}
			r = append(r, c)
		default:
			// return more objects
			for i := 100; i < 100+cnt; i++ {
				e := server.NewEntity("ex:c-"+strconv.Itoa(i), 0)
				r = append(r, e)
			}
			c := &Continuation{ID: "@continuation", Token: "lastpage"}
			r = append(r, c)
		}

		return context.JSON(http.StatusOK, r)
	})

	return result
}

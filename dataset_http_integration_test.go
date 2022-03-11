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

package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/labstack/echo/v4"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/franela/goblin"
	"go.uber.org/fx"

	"github.com/mimiro-io/datahub/internal/server"
)

func TestFullSync(t *testing.T) {
	g := goblin.Goblin(t)

	var app *fx.App
	var mockLayer MockLayer

	location := "./dataset_fullsync_integration_test"
	dsUrl := "http://localhost:24997/datasets/bananas"
	proxyDsUrl := "http://localhost:24997/datasets/cucumbers"
	datasetsUrl := "http://localhost:24997/datasets"

	g.Describe("The dataset endpoint", func() {
		g.Before(func() {
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
			app, _ = Start(context.Background())
			os.Stdout = oldOut
			os.Stderr = oldErr
			mockLayer = NewMockLayer()
			go func() {
				_ = mockLayer.echo.Start(":7778")
			}()
		})
		g.After(func() {
			mockLayer.echo.Shutdown(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			err := app.Stop(ctx)
			defer cancel()
			g.Assert(err).IsNil()
			err = os.RemoveAll(location)
			g.Assert(err).IsNil()
			_ = os.Unsetenv("STORE_LOCATION")
			_ = os.Unsetenv("PROFILE")
			_ = os.Unsetenv("SERVER_PORT")
			_ = os.Unsetenv("FULLSYNC_LEASE_TIMEOUT")
		})

		g.Describe("The dataset root API", func() {

			g.It("Should create a regular dataset", func() {
				// create new dataset
				res, err := http.Post(dsUrl, "application/json", strings.NewReader(""))
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
			})
			g.It("Should retrieve a regular dataset", func() {
				res, err := http.Get(dsUrl)
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				b, _ := io.ReadAll(res.Body)
				m := map[string]interface{}{}
				_ = json.Unmarshal(b, &m)
				g.Assert(m["id"]).Eql("ns0:bananas")
				refs := m["refs"].(map[string]interface{})
				g.Assert(refs["ns2:type"]).Eql("ns1:dataset")
			})
			g.It("Should create a proxy dataset", func() {
				// create new dataset
				res, err := http.Post(proxyDsUrl+"?proxy=true", "application/json", strings.NewReader(
					"{'dataset-id': 'proxy-test-ds'}"))
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
			})
			g.It("Should retrieve a proxy dataset", func() {
				res, err := http.Get(proxyDsUrl)
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				b, _ := io.ReadAll(res.Body)
				m := map[string]interface{}{}
				_ = json.Unmarshal(b, &m)
				g.Assert(m["id"]).Eql("ns0:cucumbers")
				refs := m["refs"].(map[string]interface{})
				g.Assert(refs["ns2:type"]).Eql("ns1:proxy-dataset")
			})
			g.It("Should list both regular and proxy datasets", func() {
				res, err := http.Get(datasetsUrl)
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				b, _ := io.ReadAll(res.Body)
				var l []map[string]interface{}
				_ = json.Unmarshal(b, &l)
				g.Assert(len(l)).Eql(3, "core.Dataset, bananas, cucumbers are listed")
			})
		})
		g.Describe("The /entities and /changes API endpoints for regular datasets", func() {

			g.It("Should accept a single batch of changes", func() {
				// populate dataset
				payload := strings.NewReader(bananasFromTo(1, 10, false))
				res, err := http.Post(dsUrl+"/entities", "application/json", payload)

				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// read it back
				res, err = http.Get(dsUrl + "/changes")
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				bodyBytes, err := ioutil.ReadAll(res.Body)
				var entities []*server.Entity
				err = json.Unmarshal(bodyBytes, &entities)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(12, "expected 10 entities plus @context and @continuation")
			})

			g.It("Should accept multiple overlapping batches of changes", func() {
				// replace 5-10 and add 11-15
				payload := strings.NewReader(bananasFromTo(5, 15, false))
				res, err := http.Post(dsUrl+"/entities", "application/json", payload)
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// replace 10-15 and add 16-20
				payload = strings.NewReader(bananasFromTo(10, 20, false))
				res, err = http.Post(dsUrl+"/entities", "application/json", payload)
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// read it back
				res, err = http.Get(dsUrl + "/changes")
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				var entities []*server.Entity
				err = json.Unmarshal(bodyBytes, &entities)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(22, "expected 20 entities plus @context and @continuation")
			})

			g.It("Should record deleted states", func() {
				payload := strings.NewReader(bananasFromTo(7, 8, true))
				res, err := http.Post(dsUrl+"/entities", "application/json", payload)
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// read changes back
				res, err = http.Get(dsUrl + "/changes")
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				var entities []*server.Entity
				err = json.Unmarshal(bodyBytes, &entities)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(24, "expected 20 entities plus 2 deleted-changes plus @context and @continuation")
				g.Assert(entities[7].IsDeleted).IsFalse("original change 7 is still undeleted")
				g.Assert(entities[22].IsDeleted).IsTrue("deleted state for 7  is a new change at end of list")

				// read entities back
				res, err = http.Get(dsUrl + "/entities")
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ = ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				entities = nil
				err = json.Unmarshal(bodyBytes, &entities)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(22, "expected 20 entities plus @context and @continuation")
				g.Assert(entities[7].IsDeleted).IsTrue("entity 7 is deleted")
			})

			g.It("Should do deletion detection in a fullsync", func() {
				// only send IDs 4 through 16 in batches as fullsync
				// 1-3 and 17-20 should end up deleted

				// first batch with "start" header
				payload := strings.NewReader(bananasFromTo(4, 8, false))
				ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ := http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-start", "true")
				req.Header.Add("universal-data-api-full-sync-id", "42")
				_, err := http.DefaultClient.Do(req)
				g.Assert(err).IsNil()
				cancel()

				// 2nd batch
				payload = strings.NewReader(bananasFromTo(9, 12, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-id", "42")
				_, _ = http.DefaultClient.Do(req)
				cancel()

				// last batch with "end" signal
				payload = strings.NewReader(bananasFromTo(13, 16, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-id", "42")
				req.Header.Add("universal-data-api-full-sync-end", "true")
				_, _ = http.DefaultClient.Do(req)
				cancel()

				// read changes back
				res, err := http.Get(dsUrl + "/changes")
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				var entities []*server.Entity
				err = json.Unmarshal(bodyBytes, &entities)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(33, "expected 20 entities plus 11 changes and @context and @continuation")
				g.Assert(entities[7].IsDeleted).IsFalse("original change 7 is still undeleted")
				g.Assert(entities[21].IsDeleted).IsTrue("deleted state for 7  is a new change at end of list")

				// read entities back
				res, err = http.Get(dsUrl + "/entities")
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ = ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				entities = nil
				err = json.Unmarshal(bodyBytes, &entities)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(22, "expected 20 entities plus @context and @continuation")
				// remove context
				entities = entities[1:]
				for i := 0; i < 3; i++ {
					g.Assert(entities[i].IsDeleted).IsTrue("entity was not part of fullsync, should be deleted: ", i)
				}
				for i := 3; i < 16; i++ {
					g.Assert(entities[i].IsDeleted).IsFalse("entity was part of fullsync, should be active: ", i)
				}
				for i := 16; i < 20; i++ {
					g.Assert(entities[i].IsDeleted).IsTrue("entity was not part of fullsync, should be deleted: ", i)
				}
			})

			g.It("should keep fullsync requests with same sync-id in parallel", func() {
				// only send IDs 4 through 16 in batches as fullsync
				// 1-3 and 17-20 should end up deleted

				// first batch with "start" header
				payload := strings.NewReader(bananasFromTo(4, 4, false))
				ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ := http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-start", "true")
				req.Header.Add("universal-data-api-full-sync-id", "43")
				_, _ = http.DefaultClient.Do(req)
				cancel()

				// next, updated id 5 with wrong sync-id. should not be registered as "seen" and therefore be deleted after fs
				payload = strings.NewReader(bananasFromTo(5, 5, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-id", "44")
				res, err := http.DefaultClient.Do(req)
				cancel()
				g.Assert(res.StatusCode).Eql(409, "request should be rejected because fullsync is going on")

				// also try to add id 5 without sync-id. should still be rejected
				payload = strings.NewReader(bananasFromTo(5, 5, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				res, err = http.DefaultClient.Do(req)
				cancel()
				g.Assert(res.StatusCode).Eql(409, "request should be rejected because fullsync is going on")

				// 10 batches in parallel with correct sync-id
				wg := sync.WaitGroup{}
				for i := 6; i < 16; i++ {
					wg.Add(1)
					id := i
					go func() {
						payload := strings.NewReader(bananasFromTo(id, id, false))
						ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
						req, _ := http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
						req.Header.Add("universal-data-api-full-sync-id", "43")
						_, _ = http.DefaultClient.Do(req)
						cancel()
						wg.Done()
					}()
				}

				wg.Wait()

				// last batch with "end" signal
				payload = strings.NewReader(bananasFromTo(16, 16, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-id", "43")
				req.Header.Add("universal-data-api-full-sync-end", "true")
				_, _ = http.DefaultClient.Do(req)
				cancel()

				// read changes back
				res, err = http.Get(dsUrl + "/changes")
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				var entities []*server.Entity
				err = json.Unmarshal(bodyBytes, &entities)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(34, "expected 31 changes from before plus deletion of id5 and @context and @continuation")
				g.Assert(entities[32].IsDeleted).IsTrue("deleted state for 5  is a new change at end of list")
			})
			g.It("should abandon fullsync when new fullsync is started", func() {
				// start a fullsync
				payload := strings.NewReader(bananasFromTo(1, 1, false))
				ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ := http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-start", "true")
				req.Header.Add("universal-data-api-full-sync-id", "45")
				res, err := http.DefaultClient.Do(req)
				cancel()
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// start another fullsync
				payload = strings.NewReader(bananasFromTo(1, 1, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-start", "true")
				req.Header.Add("universal-data-api-full-sync-id", "46")
				res, err = http.DefaultClient.Do(req)
				cancel()
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// try to append to first fullsync, should be rejected
				payload = strings.NewReader(bananasFromTo(2, 2, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-id", "45")
				res, err = http.DefaultClient.Do(req)
				cancel()
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(409, "expect rejection since syncid 45 is not active anymore")

				// complete second sync
				payload = strings.NewReader(bananasFromTo(16, 16, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-id", "46")
				req.Header.Add("universal-data-api-full-sync-end", "true")
				res, err = http.DefaultClient.Do(req)
				cancel()
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200, "sync 46 accept requests")
			})
			g.It("should abandon fullsync after a timeout period without new requests", func() {
				// start a fullsync
				payload := strings.NewReader(bananasFromTo(1, 1, false))
				ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ := http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-start", "true")
				req.Header.Add("universal-data-api-full-sync-id", "47")
				res, err := http.DefaultClient.Do(req)
				cancel()
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// exceed fullsync timeout
				time.Sleep(501 * time.Millisecond)

				// send next fullsync batch. should be OK even though lease is timed out
				payload = strings.NewReader(bananasFromTo(2, 2, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-id", "47")
				res, err = http.DefaultClient.Do(req)
				cancel()
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// send next end signal. should produce error since lease should have timed out
				payload = strings.NewReader(bananasFromTo(3, 3, false))
				ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
				req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
				req.Header.Add("universal-data-api-full-sync-end", "true")
				req.Header.Add("universal-data-api-full-sync-id", "47")
				res, err = http.DefaultClient.Do(req)
				cancel()
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(410)
			})

			g.It("Should pageinate over entities with continuation token", func() {
				payload := strings.NewReader(bananasFromTo(1, 100, false))
				res, err := http.Post(dsUrl+"/entities", "application/json", payload)
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)

				// read first page of 10 entities back
				res, err = http.Get(dsUrl + "/entities?limit=10")
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				var entities []*server.Entity
				err = json.Unmarshal(bodyBytes, &entities)
				var m []map[string]interface{}
				json.Unmarshal(bodyBytes, &m)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(12, "expected 10 entities plus @context and @continuation")
				g.Assert(entities[1].ID).Eql("ns3:1")
				token := m[11]["token"].(string)

				// read next page
				res, err = http.Get(dsUrl + "/entities?limit=90&from=" + url.QueryEscape(token))
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ = ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				err = json.Unmarshal(bodyBytes, &entities)
				json.Unmarshal(bodyBytes, &m)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(92, "expected 90 entities plus @context and @continuation")
				g.Assert(entities[1].ID).Eql("ns3:11")
				token = m[91]["token"].(string)

				// read next page after all consumed
				res, err = http.Get(dsUrl + "/entities?limit=10&from=" + url.QueryEscape(token))
				g.Assert(err).IsNil()
				g.Assert(res).IsNotZero()
				g.Assert(res.StatusCode).Eql(200)
				bodyBytes, _ = ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				err = json.Unmarshal(bodyBytes, &entities)
				json.Unmarshal(bodyBytes, &m)
				g.Assert(err).IsNil()
				g.Assert(len(entities)).Eql(2, "expected 0 entities plus @context and @continuation")
				g.Assert(entities[1].ID).Eql("@continuation")
			})
		})
		g.Describe("The /changes and /entities endpoints for proxy datasets", func() {
			g.It("Should fetch from remote for GET /changes without token")
			g.It("Should fetch from remote for GET /changes with token")
			g.It("Should fetch from remote for GET /entities without token")
			g.It("Should fetch from remote for GET /entities with token")
			g.It("Should push to remote for POST /entities")
		})
	})
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

type MockLayer struct {
	RecordedEntities map[string][]*server.Entity
	echo             *echo.Echo
}

func (m MockLayer) getContinuationTokenForDataset(dsName string) string {
	token := ""
	for _, e := range m.RecordedEntities[dsName] {
		if e.ID == "@continuation" {
			//continue so that we find the last token
			token = fmt.Sprintf("%v", e.Properties)
		}
	}
	return token
}

func (m MockLayer) getRecordedEntitiesForDataset(dsName string) []*server.Entity {
	allEntities := m.RecordedEntities[dsName]
	var readEntities []*server.Entity
	for _, e := range allEntities {
		if e.ID != "@context" {
			readEntities = append(readEntities, e)
		}
	}
	return readEntities
}

type Continuation struct {
	Id    string `json:"id"`
	Token string `json:"token"`
}

func NewMockLayer() MockLayer {
	e := echo.New()
	result := MockLayer{}
	result.RecordedEntities = make(map[string][]*server.Entity)
	result.echo = e
	e.HideBanner = true

	ctx := make(map[string]interface{})
	ctx["id"] = "@context"
	ns := make(map[string]string)
	ns["ex"] = "http://example.mimiro.io/"
	ns["_"] = "http://default.mimiro.io/"
	ctx["namespaces"] = ns

	e.GET("/datasets/people/changes", func(context echo.Context) error {
		result := make([]interface{}, 0)
		result = append(result, ctx)

		// add some objects
		for i := 0; i < 10; i++ {
			e := server.NewEntity("ex:e-"+strconv.Itoa(i), 0)
			result = append(result, e)
		}
		return context.JSON(http.StatusOK, result)
	})

	e.GET("/datasets/people/changeswithcontinuation", func(context echo.Context) error {
		result := make([]interface{}, 0)
		result = append(result, ctx)

		// check for since
		since := context.QueryParam("since")
		if since == "" {
			// add some objects
			for i := 10; i < 20; i++ {
				e := server.NewEntity("ex:e-"+strconv.Itoa(i), 0)
				result = append(result, e)
			}
			c := &Continuation{Id: "@continuation", Token: "nextplease"}
			result = append(result, c)
		} else {
			// return more objects
			for i := 0; i < 10; i++ {
				e := server.NewEntity("ex:e-"+strconv.Itoa(i), 0)
				result = append(result, e)
			}
		}

		return context.JSON(http.StatusOK, result)
	})

	return result
}

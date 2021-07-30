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

package server

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"

	"github.com/franela/goblin"

	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"
)

func TestJsonOmitOnEntity(m *testing.T) {
	e := Entity{}
	jsonData, _ := json.Marshal(e)
	jsonStr := string(jsonData)
	if jsonStr != "{\"refs\":null,\"props\":null}" {
		m.FailNow()
	}
}

func TestStreamParser(m *testing.T) {
	g := goblin.Goblin(m)
	g.Describe("The stream parser", func() {
		testCnt := 0
		var store *Store
		var storeLocation string
		g.BeforeEach(func() {

			testCnt += 1
			storeLocation = fmt.Sprintf("./test_streamparser_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}

			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(m)
			store = NewStore(lc, e, &statsd.NoOpClient{})
			lc.RequireStart()
			os.Stderr = oldErr
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("Should deal with @context, data and @continuation entities", func() {
			reader := strings.NewReader(`[
				{ "id" : "@context",
					"namespaces" : {
						"mimiro-people" : "http://data.mimiro.io/people/",
						"_" : "http://data.mimiro.io/core/"
					}
				}, {
					"id" : "mimiro-people:homer",
					"props" : { "Name" : "Homer Simpson" },
					"refs" : { "friends" : [ "mimiro-people:jon" , "mimiro-people:james"] }
				}, {
					"id" : "@continuation",
					"token" : "next-20"
				} ]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNil("Parsing produced an unexpected error")
			g.Assert(len(entities)).Eql(2, "Parser did produce wrong number of entities")

			cont := entities[1]
			g.Assert(cont.ID).Eql("@continuation", "Second and last entity was not continuation")
			g.Assert(cont.GetStringProperty("token")).Eql("next-20", "Unexpected token value")
		})

		g.It("Should detect bad/missing entity ids", func() {
			reader := strings.NewReader(` [ {
					"id" : "@context",
					"namespaces" : {
						"mimiro-people" : "http://data.mimiro.io/people/",
						"_" : "http://data.mimiro.io/core/"
					}
				}, {
					"id" : "",
					"props" : { "Name" : "Homer Simpson" },
					"refs" : { "friends" : [ "mimiro-people:jon" , "mimiro-people:james"] }
				}, {
					"id" : "@continuation",
					"token" : "next-20"
				}]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNotNil("Parsing did not produce expected error")
			g.Assert(err.Error()).Eql("parsing error: Unable to parse entity: empty value not allowed")
		})

		g.It("Should detect missing default namespace in context", func() {
			reader := strings.NewReader(`[ {
					"id" : "@context",
					"namespaces" : {
						"mimiro" : "http://data.mimiro.io/core/",
						"mimiro-people" : "http://data.mimiro.io/people/"
					}
				}, {
					"id" : "mimiro:23",
					"props" : { "Name" : "Homer Simpson" },
					"refs" : { "friends" : [ "mimiro-people:jon" , "mimiro-people:james"] }
				}, {
					"id" : "@continuation",
					"token" : "next-20"
				} ]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNotNil("Parsing did not produce expected error")
			g.Assert(err.Error()).Eql("parsing error: Unable to parse entity: " +
				"unable to parse properties no expansion for default prefix _ ")
		})

		g.It("Should detect missing expansions", func() {
			reader := strings.NewReader(` [ {
					"id" : "@context",
					"namespaces" : {
						"mimiro" : "http://data.mimiro.io/core/",
						"mimiro-people" : "http://data.mimiro.io/people/"
					}
				}, {
					"id" : "woddle:23",
					"props" : { "Name" : "Homer Simpson" },
					"refs" : { "friends" : [ "mimiro-people:jon" , "mimiro-people:james"] }
				}, {
					"id" : "@continuation",
					"token" : "next-20"
				} ]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNotNil("Parsing did not produce expected error")
			g.Assert(err.Error()).Eql("parsing error: Unable to parse entity: no expansion for prefix woddle")
		})

		g.It("Should accept empty properties and empty refs", func() {
			reader := strings.NewReader(` [ {
					"id" : "@context",
					"namespaces" : {
						"mimiro" : "http://data.mimiro.io/core/",
						"mimiro-people" : "http://data.mimiro.io/people/"
					} },
				{ "id" : "mimiro-people:23", "props" : { } },
				{ "id" : "mimiro-people:24", "refs" : { } },
				{ "id" : "mimiro-people:25" },
				{ "id" : "mimiro-people:26", "refs" : { }, "props" : { } },
				{ "id" : "@continuation", "token" : "next-20" } ]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNil("Parsing produced unexpected error")
			g.Assert(len(entities)).Eql(5, "Expect all entities to be represented")
		})

		g.It("Should handle nested entities", func() {
			reader := strings.NewReader(` [ {
					"id" : "@context",
					"namespaces" : {
						"_" : "http://data.mimiro.io/core/",
						"people" : "http://data.mimiro.io/people/"
					}
				}, {
					"id" : "people:23",
					"props" : {
						"address" : {
							"props" : {
								"line1" : "earth"
							}
						}
					}
				} ]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNil("Parsing produced unexpected error")
			g.Assert(len(entities)).Eql(1, "Expected correnct number of entitites")
			addressSeen := false
			for k, v := range entities[0].Properties {
				if strings.HasSuffix(k, "address") {
					addressSeen = true
					subEntity, castOK := v.(*Entity)
					g.Assert(castOK).IsTrue("Expected address value to be another entity instance")
					g.Assert(len(subEntity.Properties)).Eql(1, "Expected sub-entity to have one property (line1)")
				}
			}
			g.Assert(addressSeen).IsTrue("Expected address property to be mapped")
		})

		g.It("Should handle nested entities with arrays", func() {
			reader := strings.NewReader(` [ {
					"id" : "@context",
					"namespaces" : {
						"_" : "http://data.mimiro.io/core/",
						"people" : "http://data.mimiro.io/people/"
					}
				}, {
					"id" : "people:23",
					"props" : {
						"address" : {
							"props" : {
								"line1" : "earth"
							}
						}
					}
				} ]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNil("Parsing produced unexpected error")
			g.Assert(len(entities)).Eql(1, "Expected correnct number of entitites")
			addressSeen := false
			for k, v := range entities[0].Properties {
				if strings.HasSuffix(k, "address") {
					addressSeen = true
					subEntity, castOK := v.(*Entity)
					g.Assert(castOK).IsTrue("Expected address value to be another entity instance")
					g.Assert(len(subEntity.Properties)).Eql(1, "Expected sub-entity to have one property (line1)")
				}
			}
			g.Assert(addressSeen).IsTrue("Expected address property to be mapped")
		})

		g.It("Should handle nested entities with id", func() {
			reader := strings.NewReader(` [ {
					"id" : "@context",
					"namespaces" : {
						"_" : "http://data.mimiro.io/core/",
						"people" : "http://data.mimiro.io/people/",
						"addresses" : "http://data.mimiro.io/addresses/"
					}
				}, {
					"id" : "people:23",
					"props" : {
						"address" : {
							"id" : "addresses:44",
							"props" : {
								"line1" : "earth"
							}
						}
					}
				}
			]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNil("Parsing produced unexpected error")
			g.Assert(len(entities)).Eql(1, "Expected correnct number of entitites")
			addressSeen := false
			for k, v := range entities[0].Properties {
				if strings.HasSuffix(k, "address") {
					addressSeen = true
					subEntity, castOK := v.(*Entity)
					g.Assert(castOK).IsTrue("Expected address value to be another entity instance")
					g.Assert(len(subEntity.Properties)).Eql(1, "Expected sub-entity to have one property (line1)")
					g.Assert(strings.HasSuffix(subEntity.ID, "44")).IsTrue("Expected sub-entity ID")
				}
			}
			g.Assert(addressSeen).IsTrue("Expected address property to be mapped")
		})

		g.It("Should Ignore keys outside props and refs", func() {
			reader := strings.NewReader(` [ {
					"id" : "@context",
					"namespaces" : {
						"_" : "http://data.mimiro.io/core/",
						"people" : "http://data.mimiro.io/people/",
						"addresses" : "http://data.mimiro.io/addresses/"
					}
				},
				{
					"id" : "people:23",
					"props" : {
						"address" : {
							"id" : "addresses:44",
							"line1" : "earth"
						}
					}
				}
			]`)

			esp := NewEntityStreamParser(store)
			entities := make([]*Entity, 0)
			err := esp.ParseStream(reader, func(e *Entity) error {
				entities = append(entities, e)
				return nil
			})
			g.Assert(err).IsNil("Parsing produced unexpected error")
			g.Assert(len(entities)).Eql(1, "Expected correct number of entitites")
			addressSeen := false
			for k, v := range entities[0].Properties {
				if strings.HasSuffix(k, "address") {
					addressSeen = true
					subEntity, castOK := v.(*Entity)
					g.Assert(castOK).IsTrue("Expected address value to be another entity instance")
					g.Assert(len(subEntity.Properties)).Eql(0, "Expected sub-entity to have no property"+
						" (line1 is not inside a props)")
					g.Assert(strings.HasSuffix(subEntity.ID, "44")).IsTrue("Expected sub-entity ID")
				}
			}
			g.Assert(addressSeen).IsTrue("Expected address property to be mapped")
		})
	})
}

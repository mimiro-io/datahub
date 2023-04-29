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

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
)

func TestJsonOmitOnEntity(m *testing.T) {
	e := Entity{}
	jsonData, _ := json.Marshal(e)
	jsonStr := string(jsonData)
	if jsonStr != "{\"refs\":null,\"props\":null}" {
		m.FailNow()
	}
}

var _ = ginkgo.Describe("The stream parser", func() {
	testCnt := 0
	var store *Store
	var storeLocation string
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_streamparser_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		e := &conf.Env{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}

		devNull, _ := os.Open("/dev/null")
		oldErr := os.Stderr
		os.Stderr = devNull
		lc := fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		store = NewStore(lc, e, &statsd.NoOpClient{})
		lc.RequireStart()
		os.Stderr = oldErr
	})
	ginkgo.AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	ginkgo.It("Should process transaction with dataset with array of entities", func() {
		reader := strings.NewReader(
			`{
						"@context" : {
							"namespaces" : {
								"mimiro-people" : "http://data.mimiro.io/people/",
								"_" : "http://data.mimiro.io/core/"
							}
						},
						"people" : [
							{
								"id" : "http://data.mimiro.io/people/12345000"
							},
							{
								"id" : "http://data.mimiro.io/people/12345999"
							}
						]
					}`)

		esp := NewEntityStreamParser(store)
		txn, err := esp.ParseTransaction(reader)

		Expect(txn).NotTo(BeNil(), "Transaction cannot be nil")
		Expect(err).To(BeNil(), "Parsing didnt produce the expected error")

		people := txn.DatasetEntities["people"]
		Expect(people).NotTo(BeNil(), "people dataset updates missing")
	})

	ginkgo.It("Should process transaction with dataset with empty array of entities", func() {
		reader := strings.NewReader(
			`{
						"@context" : {
							"namespaces" : {
								"mimiro-people" : "http://data.mimiro.io/people/",
								"_" : "http://data.mimiro.io/core/"
							}
						},
						"people" : []
					}`)

		esp := NewEntityStreamParser(store)
		txn, err := esp.ParseTransaction(reader)

		Expect(txn).NotTo(BeNil(), "Transaction cannot be nil")
		Expect(err).To(BeNil(), "Parsing didnt produce the expected error")
	})

	ginkgo.It("Should error with transactions with no context", func() {
		reader := strings.NewReader(
			`{
						"people" : [
							{
								"id" : "http://data.mimiro.io/people/12345"
							}
						]
					}`)

		esp := NewEntityStreamParser(store)
		_, err := esp.ParseTransaction(reader)

		Expect(err).NotTo(BeNil(), "Parsing didnt produce the expected error")
	})

	ginkgo.It("Should deal with transactions with only a context", func() {
		reader := strings.NewReader(
			`{
						"@context" : {
							"namespaces" : {
								"mimiro-people" : "http://data.mimiro.io/people/",
								"_" : "http://data.mimiro.io/core/"
							}
						}
					}`)

		esp := NewEntityStreamParser(store)
		txn, err := esp.ParseTransaction(reader)

		Expect(err).To(BeNil(), "Parsing produced an unexpected error")
		Expect(txn).NotTo(BeNil(), "No transaction returned")
	})

	ginkgo.It("Should deal with @context, data and @continuation entities", func() {
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
		Expect(err).To(BeNil(), "Parsing produced an unexpected error")
		Expect(len(entities)).To(Equal(2), "Parser did produce wrong number of entities")

		cont := entities[1]
		Expect(cont.ID).To(Equal("@continuation"), "Second and last entity was not continuation")
		Expect(cont.GetStringProperty("token")).To(Equal("next-20"), "Unexpected token value")
	})

	ginkgo.It("Should detect bad/missing entity ids", func() {
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
		Expect(err).NotTo(BeNil(), "Parsing did not produce expected error")
		Expect(err.Error()).To(Equal("parsing error: Unable to parse entity: empty value not allowed"))
	})

	ginkgo.It("Should detect missing default namespace in context", func() {
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
		Expect(err).NotTo(BeNil(), "Parsing did not produce expected error")
		Expect(err.Error()).To(Equal("parsing error: Unable to parse entity: " +
			"unable to parse properties no expansion for default prefix _ "))
	})

	ginkgo.It("Should detect missing expansions", func() {
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
		Expect(err).NotTo(BeNil(), "Parsing did not produce expected error")
		Expect(err.Error()).To(Equal("parsing error: Unable to parse entity: no expansion for prefix woddle"))
	})

	ginkgo.It("Should accept empty properties and empty refs", func() {
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
		Expect(err).To(BeNil(), "Parsing produced unexpected error")
		Expect(len(entities)).To(Equal(5), "Expect all entities to be represented")
	})

	ginkgo.It("Should handle nested entities", func() {
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
		Expect(err).To(BeNil(), "Parsing produced unexpected error")
		Expect(len(entities)).To(Equal(1), "Expected correnct number of entitites")
		addressSeen := false
		for k, v := range entities[0].Properties {
			if strings.HasSuffix(k, "address") {
				addressSeen = true
				subEntity, castOK := v.(*Entity)
				Expect(castOK).To(BeTrue(), "Expected address value to be another entity instance")
				Expect(len(subEntity.Properties)).To(Equal(1), "Expected sub-entity to have one property (line1)")
			}
		}
		Expect(addressSeen).To(BeTrue(), "Expected address property to be mapped")
	})

	ginkgo.It("Should handle nested entities with arrays", func() {
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
		Expect(err).To(BeNil(), "Parsing produced unexpected error")
		Expect(len(entities)).To(Equal(1), "Expected correnct number of entitites")
		addressSeen := false
		for k, v := range entities[0].Properties {
			if strings.HasSuffix(k, "address") {
				addressSeen = true
				subEntity, castOK := v.(*Entity)
				Expect(castOK).To(BeTrue(), "Expected address value to be another entity instance")
				Expect(len(subEntity.Properties)).To(Equal(1), "Expected sub-entity to have one property (line1)")
			}
		}
		Expect(addressSeen).To(BeTrue(), "Expected address property to be mapped")
	})

	ginkgo.It("Should handle nested entities with id", func() {
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
		Expect(err).To(BeNil(), "Parsing produced unexpected error")
		Expect(len(entities)).To(Equal(1), "Expected correnct number of entitites")
		addressSeen := false
		for k, v := range entities[0].Properties {
			if strings.HasSuffix(k, "address") {
				addressSeen = true
				subEntity, castOK := v.(*Entity)
				Expect(castOK).To(BeTrue(), "Expected address value to be another entity instance")
				Expect(len(subEntity.Properties)).To(Equal(1), "Expected sub-entity to have one property (line1)")
				Expect(strings.HasSuffix(subEntity.ID, "44")).To(BeTrue(), "Expected sub-entity ID")
			}
		}
		Expect(addressSeen).To(BeTrue(), "Expected address property to be mapped")
	})

	ginkgo.It("Should Ignore keys outside props and refs", func() {
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
		Expect(err).To(BeNil(), "Parsing produced unexpected error")
		Expect(len(entities)).To(Equal(1), "Expected correct number of entitites")
		addressSeen := false
		for k, v := range entities[0].Properties {
			if strings.HasSuffix(k, "address") {
				addressSeen = true
				subEntity, castOK := v.(*Entity)
				Expect(castOK).To(BeTrue(), "Expected address value to be another entity instance")
				Expect(len(subEntity.Properties)).To(Equal(0), "Expected sub-entity to have no property"+
					" (line1 is not inside a props)")
				Expect(strings.HasSuffix(subEntity.ID, "44")).To(BeTrue(), "Expected sub-entity ID")
			}
		}
		Expect(addressSeen).To(BeTrue(), "Expected address property to be mapped")
	})
})

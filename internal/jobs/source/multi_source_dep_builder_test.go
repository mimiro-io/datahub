// Copyright 2023 MIMIRO AS
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

package source_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/DataDog/datadog-go/v5/statsd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/server"
)

var _ = Describe("parseDependencies", func() {
	testCnt := 0
	var dsm *server.DsManager
	var store *server.Store
	var scheduler *jobs.Scheduler
	var storeLocation string
	BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_multi_source_dep_builder_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)

		e := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
			RunnerConfig:  &conf.RunnerConfig{},
		}
		// lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))

		store = server.NewStore(e, &statsd.NoOpClient{})
		dsm = server.NewDsManager(e, store, server.NoOpBus())

		devNull, _ := os.Open("/dev/null")
		oldStd := os.Stdout
		os.Stdout = devNull
		runner := jobs.NewRunner(e, store, nil, nil, &statsd.NoOpClient{})
		scheduler = jobs.NewScheduler(e, store, dsm, runner)
		os.Stdout = oldStd

		/*err = lc.Start(context.Background())
		os.Stdout = oldStd
		if err != nil {
			fmt.Println(err.Error())
			Fail(err.Error())
		} */
	})
	AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})
	It("should translate json to config", func() {
		s := source.MultiSource{DatasetName: "person", Store: store, DatasetManager: dsm}
		srcJSON := `{
				"Type" : "MultiSource",
				"Name" : "person",
				"Dependencies": [
					{
						"dataset": "product",
						"joins": [
							{
								"dataset": "order",
								"predicate": "product-ordered",
								"inverse": true
							},
							{
								"dataset": "person",
								"predicate": "ordering-customer",
								"inverse": false
							}
						]
					},
					{
						"dataset": "order",
						"joins": [
							{
								"dataset": "person",
								"predicate": "ordering-customer",
								"inverse": false
							}
						]
					}
				]
			}`

		srcConfig := map[string]interface{}{}
		err := json.Unmarshal([]byte(srcJSON), &srcConfig)
		Expect(err).To(BeNil())
		err = s.ParseDependencies(srcConfig["Dependencies"], nil)
		Expect(err).To(BeNil())

		Expect(s.Dependencies).NotTo(BeZero())
		Expect(len(s.Dependencies)).To(Equal(2))

		dep := s.Dependencies[0]
		Expect(dep.Dataset).To(Equal("product"))
		Expect(dep.Joins).NotTo(BeZero())
		Expect(len(dep.Joins)).To(Equal(2))
		j := dep.Joins[0]
		Expect(j.Dataset).To(Equal("order"))
		Expect(j.Predicate).To(Equal("product-ordered"))
		Expect(j.Inverse).To(BeTrue())
		j = dep.Joins[1]
		Expect(j.Dataset).To(Equal("person"))
		Expect(j.Predicate).To(Equal("ordering-customer"))
		Expect(j.Inverse).To(BeFalse())

		dep = s.Dependencies[1]
		Expect(dep.Dataset).To(Equal("order"))
		Expect(dep.Joins).NotTo(BeZero())
		Expect(len(dep.Joins)).To(Equal(1))
		j = dep.Joins[0]
		Expect(j.Dataset).To(Equal("person"))
		Expect(j.Predicate).To(Equal("ordering-customer"))
		Expect(j.Inverse).To(BeFalse())
	})
	It("Should fail if main dataset is proxy dataset", func() {
		// create main dataset as proxy dataset
		_, err := dsm.CreateDataset("people", &server.CreateDatasetConfig{
			ProxyDatasetConfig: &server.ProxyDatasetConfig{
				RemoteURL: "http://localhost:7777/datasets/people",
			},
		})
		Expect(err).To(BeNil())

		// now instantiate (simulating job start)
		testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
		srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [
                          {
							"dataset": "address",
							"joins": [
							  { "dataset": "office", "predicate": "http://office/location", "inverse": true },
							  { "dataset": "people", "predicate": "http://office/contact", "inverse": false },
							  { "dataset": "team", "predicate": "http://team/lead", "inverse": true },
							  { "dataset": "people", "predicate": "http://team/member", "inverse": false }
							]
                          }
                        ] }`

		srcConfig := map[string]interface{}{}
		_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
		err = testSource.ParseDependencies(srcConfig["Dependencies"], nil)
		// t.Log(err)
		Expect(err).NotTo(BeNil())
	})
	It("Should fail if a dependency is a proxy dataset", func() {
		// create dependency dataset as proxy dataset
		_, err := dsm.CreateDataset("address", &server.CreateDatasetConfig{
			ProxyDatasetConfig: &server.ProxyDatasetConfig{
				RemoteURL: "http://localhost:7777/datasets/address",
			},
		})
		Expect(err).To(BeNil())

		// now instantiate (simulating job start)
		testSource := source.MultiSource{DatasetName: "people", Store: store, DatasetManager: dsm}
		srcJSON := `{ "Type" : "MultiSource", "Name" : "people", "Dependencies": [
                          {
							"dataset": "address",
							"joins": [
							  { "dataset": "office", "predicate": "http://office/location", "inverse": true },
							  { "dataset": "people", "predicate": "http://office/contact", "inverse": false },
							  { "dataset": "team", "predicate": "http://team/lead", "inverse": true },
							  { "dataset": "people", "predicate": "http://team/member", "inverse": false }
							]
                          }
                        ] }`

		srcConfig := map[string]interface{}{}
		_ = json.Unmarshal([]byte(srcJSON), &srcConfig)
		err = testSource.ParseDependencies(srcConfig["Dependencies"], nil)
		// t.Log(err)
		Expect(err).NotTo(BeNil())
	})
	Describe("with transform registrations", func() {
		var s source.MultiSource
		// BeforeEach is run before every Entry
		BeforeEach(func() {
			s = source.MultiSource{DatasetName: "person", Store: store, DatasetManager: dsm}
			s.AddTransformDeps = scheduler.MultiSourceCodeRegistration
		})
		DescribeTable(
			"",

			// This function is the evaluation function, called for every Entry in the table
			func(dependencies string, transformInput []string,
				expectedOutput []source.Dependency, expectedErr error,
			) {
				// wrap registration statements with javascrip function and transform boilerplate
				transform := mkTransform(transformInput...)

				// unmarshal Dependencies json
				var deps any
				json.Unmarshal([]byte(dependencies), &deps)

				// do parsing
				err := s.ParseDependencies(deps, transform)
				if expectedErr == nil {
					Expect(err).To(BeNil())
					Expect(s.Dependencies).To(BeEquivalentTo(expectedOutput))
				} else {
					Expect(err).To(Equal(expectedErr))
					Expect(s.Dependencies).To(BeNil())
				}
			},
			func(dependencies any, transform any, expectedOutput []source.Dependency, expectedErr error) string {
				return fmt.Sprintf(
					"should turn json dependencies (%v)\nand track_queries (%v)\ninto: %+v (error: %v)",
					dependencies,
					transform,
					expectedOutput,
					expectedErr,
				)
			},

			// nothing
			Entry(
				nil,
				nil,                   // no json config
				nil,                   // no track_queries
				[]source.Dependency{}, // expect emtpy deps
				nil,                   // no error
			),

			// json config only
			Entry(
				nil,
				`[{"dataset": "address", "joins":[{"dataset": "person", "predicate": "home", "inverse": true}]}]`, // simple json config
				nil, // no track_queries
				mkDepResult(exp{"address", []j{{"person", "home", true}}}), // expect single dep
				nil, // no error
			),

			// track_queries only
			Entry(
				nil,
				nil,                                 // no json config
				[]string{`.hop("address", "home")`}, // single hop in track_queries
				mkDepResult(exp{"address", []j{{"person", "home", true}}}), // expect single dep
				nil, // no error
			),

			// combine json config and track_queries
			Entry(
				nil,
				`[{"dataset": "address", "joins":[{"dataset": "person", "predicate": "home", "inverse": true}]}]`, // simple json config
				[]string{`.iHop("car", "owner")`}, // single hop in track_queries
				mkDepResult(
					exp{"address", []j{{"person", "home", true}}},
					exp{"car", []j{{"person", "owner", false}}},
				), // expect single dep
				nil, // no error
			),

			// duplicate dependencies
			Entry(
				nil,
				// generate one dependency with 2 joins in json
				`[{"dataset": "product",
				   "joins": [{"dataset": "order", "predicate": "ordered", "inverse": true},
				             {"dataset": "person", "predicate": "ordering", "inverse": false}]}]`,
				// add same dep in track_queries as well
				[]string{`.iHop("order", "ordering").hop("product", "ordered")`},
				mkDepResult(
					// expect deduplicated product dependency
					exp{"product", []j{{"order", "ordered", true}, {"person", "ordering", false}}},
					// and implicit dependency on order (intermediate hop)
					exp{"order", []j{{"person", "ordering", false}}},
				),
				nil,
			),

			// complex track_queries
			Entry(
				nil,
				nil, // no json config
				[]string{
					`.hop("address", "home")`, // query from person to home address
					`.hop("address", "work")`, // to work address
					// find cars owned by person, then addresses where they are parked, then persons living there
					`.iHop("car", "owner").hop("address", "parked_at").iHop("person", "home")`,
				}, // single hop in track_queries
				mkDepResult(
					exp{"address", []j{{"person", "home", true}}},
					exp{"address", []j{{"person", "work", true}}},
					exp{"person", []j{
						// dependency tracking is reverse of query chain
						{"address", "home", false}, // we start with persons and follow to their home addresses
						{"car", "parked_at", true}, // then we follow the cars parked at those addresses
						{"person", "owner", false}, // and finally we follow the owners of those cars, back to persons
					}},
					// rest are implicit dependencies from above chain
					exp{"address", []j{{"car", "parked_at", true}, {"person", "owner", false}}},
					exp{"car", []j{{"person", "owner", false}}},
				), // expect single dep
				nil, // no error
			),
		)
	})
})

type exp struct {
	ds    string
	joins []j
}
type j struct {
	ds        string
	predicate string
	inverse   bool
}

func mkDepResult(es ...exp) []source.Dependency {
	result := []source.Dependency{}
	for _, e := range es {

		js := []source.Join{}
		for _, j := range e.joins {
			js = append(js, source.Join{
				Dataset:   j.ds,
				Predicate: j.predicate,
				Inverse:   j.inverse,
			})
		}

		result = append(result, source.Dependency{
			Dataset: e.ds,
			Joins:   js,
		},
		)
	}

	return result
}

func mkTransform(regs ...string) map[string]any {
	t := map[string]any{}
	t["Type"] = "JavascriptTransform"
	js := ""
	if len(regs) > 0 {
		js += ` function track_queries(reg) {`
		js += " Log(reg);\n"
		for _, reg := range regs {
			js += "reg" + reg + "\n"
		}
		js += "}"
	}
	jscriptEnc := base64.StdEncoding.EncodeToString([]byte(js))
	t["Code"] = jscriptEnc
	return t
}

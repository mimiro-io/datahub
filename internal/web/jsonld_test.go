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

package web

import (
	"encoding/json"
	"github.com/mimiro-io/datahub/internal/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
)

func TestDatasetSource(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Web Test Suite")
}

var _ = Describe("The JSON LD support", func() {
	It("Should support basic literals", func() {
		entity := &server.Entity{}
		entity.Properties = make(map[string]interface{})
		entity.References = make(map[string]interface{})
		entity.Properties["ex:name"] = "John Doe"
		entity.Properties["ex:age"] = 23

		jsonLd := toJSONLD(entity)

		// convert to json string
		jsonLdString, _ := json.Marshal(jsonLd)
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:name":"John Doe"`))
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:age":23`))
	})

	It("Should support lists of property literals", func() {
		entity := &server.Entity{}
		entity.Properties = make(map[string]interface{})
		entity.References = make(map[string]interface{})
		entity.Properties["ex:name"] = []string{"John Doe", "Jane Doe"}
		entity.Properties["ex:age"] = []int{23, 35}

		jsonLd := toJSONLD(entity)

		// convert to json string
		jsonLdString, _ := json.Marshal(jsonLd)
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:name":["John Doe","Jane Doe"]`))
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:age":[23,35]`))

	})

	It("Should support lists of lists", func() {
		entity := &server.Entity{}
		entity.Properties = make(map[string]interface{})
		entity.References = make(map[string]interface{})

		namesListOfLists := [][]string{{"John", "Doe"}, {"Jane", "Doe"}}

		entity.Properties["ex:name"] = namesListOfLists
		entity.Properties["ex:age"] = []int{23, 35}

		jsonLd := toJSONLD(entity)

		// convert to json string
		jsonLdString, _ := json.Marshal(jsonLd)
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:name":[["John","Doe"],["Jane","Doe"]]`))
	})

	It("Should support entity as property value", func() {
		entity := &server.Entity{}
		entity.Properties = make(map[string]interface{})
		entity.References = make(map[string]interface{})
		entity.Properties["ex:name"] = "bob"
		entity.Properties["ex:age"] = []int{23, 35}
		entity.Properties["ex:address"] = map[string]interface{}{
			"props": map[string]interface{}{
				"ex:street": "main street",
				"ex:city":   "copenhagen",
			},
			"id": "ex:address1",
		}

		jsonLd := toJSONLD(entity)

		// convert to json string
		jsonLdString, _ := json.Marshal(jsonLd)
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:address":{"@id":"ex:address1","ex:city":"copenhagen","ex:street":"main street"}`))
		Expect(string(jsonLdString)).To(ContainSubstring(`"@id":"ex:address1"`))
	})

	It("Should support lists of entities", func() {
		entity := &server.Entity{}
		entity.Properties = make(map[string]interface{})
		entity.References = make(map[string]interface{})
		entity.Properties["ex:name"] = "bob"
		entity.Properties["ex:age"] = []int{23, 35}
		entity.Properties["ex:address"] = []interface{}{
			map[string]interface{}{
				"props": map[string]interface{}{
					"ex:street": "main street",
					"ex:city":   "copenhagen",
				},
				"id": "ex:address1",
			},
			map[string]interface{}{
				"props": map[string]interface{}{
					"ex:street": "second street",
					"ex:city":   "copenhagen",
				},
				"id": "ex:address2",
			},
		}

		jsonLd := toJSONLD(entity)

		// convert to json string
		jsonLdString, _ := json.Marshal(jsonLd)
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:address":[{"@id":"ex:address1","ex:city":"copenhagen","ex:street":"main street"},{"@id":"ex:address2","ex:city":"copenhagen","ex:street":"second street"}]`))
		Expect(string(jsonLdString)).To(ContainSubstring(`"@id":"ex:address1"`))
		Expect(string(jsonLdString)).To(ContainSubstring(`"@id":"ex:address2"`))
	})

	It("Should support lists of items of different types. entity and literals", func() {
		entity := &server.Entity{}
		entity.Properties = make(map[string]interface{})
		entity.References = make(map[string]interface{})
		entity.Properties["ex:name"] = "bob"
		entity.Properties["ex:age"] = []int{23, 35}
		entity.Properties["ex:address"] = []interface{}{
			map[string]interface{}{
				"props": map[string]interface{}{
					"ex:street": "main street",
					"ex:city":   "copenhagen",
				},
				"id": "ex:address1",
			},
			"second street",
		}

		jsonLd := toJSONLD(entity)

		// convert to json string
		jsonLdString, _ := json.Marshal(jsonLd)
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:address":[{"@id":"ex:address1","ex:city":"copenhagen","ex:street":"main street"},"second street"]`))
		Expect(string(jsonLdString)).To(ContainSubstring(`"@id":"ex:address1"`))
	})

	It("Should support property where value is entity, but entity has no id property", func() {
		entity := &server.Entity{}
		entity.Properties = make(map[string]interface{})
		entity.References = make(map[string]interface{})
		entity.Properties["ex:name"] = "bob"
		entity.Properties["ex:age"] = []int{23, 35}
		entity.Properties["ex:address"] = map[string]interface{}{
			"props": map[string]interface{}{
				"ex:street": "main street",
				"ex:city":   "copenhagen",
			},
		}

		jsonLd := toJSONLD(entity)

		// convert to json string
		jsonLdString, _ := json.Marshal(jsonLd)
		Expect(string(jsonLdString)).To(ContainSubstring(`"ex:address":{"ex:city":"copenhagen","ex:street":"main street"}`))
	})
})

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

package jobs

import (
	"encoding/base64"
	"strings"

	"github.com/DataDog/datadog-go/v5/statsd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/server"
)

var _ = Describe("A Javascript transformation", func() {
	It("Should return a go error when the script fails", func() {
		// a failing function
		js := ` // fail.js
					function transform_entities(entities) {
						for (e of entities) {
							var bodyEvent = GetProperty(e, prefix, "failField");
							Log(bodyEvent.substring(0, 3));
					  	}
						return entities;
					}`
		f := base64.StdEncoding.EncodeToString([]byte(js))

		transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), f, nil, nil)
		Expect(err).To(BeNil(), "Expected transform runner to be created without error")

		entities := make([]*server.Entity, 0)
		entities = append(entities, server.NewEntity("1", 1))

		_, err = transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
		Expect(err).NotTo(BeNil(), "transform should fail")
		Expect(strings.Split(err.Error(), " (")[0]).
			To(Equal("ReferenceError: prefix is not defined at transform_entities"))
	})
	It("Should return a go error when the script emits non-entities", func() {
		// a failing function
		js := ` function transform_entities(entities) {
						return ["hello"];
					}`
		f := base64.StdEncoding.EncodeToString([]byte(js))

		transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), f, nil, nil)
		Expect(err).To(BeNil(), "Expected transform runner to be created without error")

		entities := make([]*server.Entity, 0)
		entities = append(entities, server.NewEntity("1", 1))

		_, err = transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
		Expect(err).NotTo(BeNil(), "transform should fail")
		Expect(strings.Split(err.Error(), " (")[0]).To(Equal("transform emitted invalid entity: hello"))
	})
	It("Should produce type compatible number properties", func() {
		// goja stores numbers without decimals as int64, we need float64 to be type-compatible with
		// json deserialized entities
		js := ` function transform_entities(entities) {

						for (e of entities) {
							SetProperty(e, "b", "output", GetProperty(e, "a", "input"))
						}
						return entities;
					}`
		f := base64.StdEncoding.EncodeToString([]byte(js))

		transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), f, nil, nil)
		Expect(err).To(BeNil(), "Expected transform runner to be created without error")

		entities := make([]*server.Entity, 0)
		entities = append(entities, server.NewEntityFromMap(map[string]any{
			"id": "1",
			"props": map[string]any{
				"a:input": float64(6708238),
			},
			"refs": map[string]any{},
		}))

		r, err := transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
		Expect(err).To(BeNil())
		Expect(len(r)).To(Equal(1))
		Expect(r[0].Properties["b:output"]).To(Equal(entities[0].Properties["a:input"]))
	})
	It("Should produce type compatible numbers in nested entities", func() {
		js := ` function transform_entities(entities) {
						for (e of entities) {
							const n = NewEntity();
							SetProperty(n, "b", "num", GetProperty(e, "a", "input"));
							SetProperty(e, "b", "output", n);
						}
						return entities;
					}`
		f := base64.StdEncoding.EncodeToString([]byte(js))

		transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), f, nil, nil)
		Expect(err).To(BeNil(), "Expected transform runner to be created without error")

		entities := make([]*server.Entity, 0)
		entities = append(entities, server.NewEntityFromMap(map[string]any{
			"id": "1",
			"props": map[string]any{
				"a:input": float64(6708238),
			},
			"refs": map[string]any{},
		}))

		r, _ := transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
		Expect(len(r)).To(Equal(1))
		// t.Logf("%+v", r[0])
		Expect(r[0].Properties["b:output"].(*server.Entity).
			Properties["b:num"]).To(Equal(entities[0].Properties["a:input"]))
	})
	It("Should produce type compatible numbers in value array properties", func() {
		js := ` function transform_entities(entities) {
						for (e of entities) {
							const val = GetProperty(e, "a", "input");
							SetProperty(e, "b", "output", [val, val]);
						}
						return entities;
					}`
		f := base64.StdEncoding.EncodeToString([]byte(js))

		transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), f, nil, nil)
		Expect(err).To(BeNil(), "Expected transform runner to be created without error")

		entities := make([]*server.Entity, 0)
		entities = append(entities, server.NewEntityFromMap(map[string]any{
			"id": "1",
			"props": map[string]any{
				"a:input": float64(6708238),
			},
			"refs": map[string]any{},
		}))

		r, err := transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
		Expect(err).To(BeNil())
		Expect(len(r)).To(Equal(1))
		// t.Logf("%+v", r[0])
		Expect(r[0].Properties["b:output"]).To(Equal([]interface{}{
			entities[0].Properties["a:input"],
			entities[0].Properties["a:input"],
		}))
	})
})

var _ = Describe("Calling ToString from a transform", func() {
	It("should return null when nil", func() {
		transform := &JavascriptTransform{}
		ret := transform.ToString(nil)

		Expect(ret).To(Equal("undefined"))
	})
	It("number should be formatted as a number", func() {
		transform := &JavascriptTransform{}
		ret := transform.ToString(30)
		Expect(ret).To(Equal("30"))

		ret = transform.ToString(2.1)
		Expect(ret).To(Equal("2.1"))
	})
	It("bool should be formatted with true or false", func() {
		transform := &JavascriptTransform{}
		ret := transform.ToString(true)
		Expect(ret).To(Equal("true"))
	})
	It("a map should correctly formatted", func() {
		transform := &JavascriptTransform{}

		lemap := map[string]interface{}{
			"field1": 1,
			"field2": "2",
		}

		ret := transform.ToString(lemap)
		Expect(ret).To(Equal("map[field1:1 field2:2]"))
	})
	It("an entity should be formatted correctly", func() {
		transform := &JavascriptTransform{}

		e := transform.NewEntity()
		e.ID = "ns1:1"
		e.InternalID = 1
		e.Recorded = 2
		e.Properties = map[string]interface{}{
			"field1": "hello",
		}
		ret := transform.ToString(e)

		Expect(ret).To(Equal("&{map[] map[field1:hello] ns1:1 1 2 false}"))
	})
})

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
	"github.com/DataDog/datadog-go/v5/statsd"
	"strings"
	"testing"

	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/zap"
)

func TestTransform(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("A Javascript transformation", func() {
		g.It("Should return a go error when the script fails", func() {
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

			transform, err := newJavascriptTransform(zap.NewNop().Sugar(), f, nil)
			g.Assert(err).IsNil("Expected transform runner to be created without error")

			entities := make([]*server.Entity, 0)
			entities = append(entities, server.NewEntity("1", 1))

			_, err = transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
			g.Assert(err).IsNotNil("transform should fail")
			g.Assert(strings.Split(err.Error(), " (")[0]).
				Eql("ReferenceError: prefix is not defined at transform_entities")
		})
		g.It("Should return a go error when the script emits non-entities", func() {
			// a failing function
			js := ` function transform_entities(entities) {
						return ["hello"];
					}`
			f := base64.StdEncoding.EncodeToString([]byte(js))

			transform, err := newJavascriptTransform(zap.NewNop().Sugar(), f, nil)
			g.Assert(err).IsNil("Expected transform runner to be created without error")

			entities := make([]*server.Entity, 0)
			entities = append(entities, server.NewEntity("1", 1))

			_, err = transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
			g.Assert(err).IsNotNil("transform should fail")
			g.Assert(strings.Split(err.Error(), " (")[0]).
				Eql("transform emitted invalid entity: hello")
		})
	})
}

func TestJavascriptTransform_ToString(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("Calling ToString from a transform", func() {
		g.It("should return null when nil", func() {
			transform := &JavascriptTransform{}
			ret := transform.ToString(nil)

			g.Assert(ret).Equal("undefined")
		})
		g.It("number should be formatted as a number", func() {
			transform := &JavascriptTransform{}
			ret := transform.ToString(30)
			g.Assert(ret).Equal("30")

			ret = transform.ToString(2.1)
			g.Assert(ret).Equal("2.1")
		})
		g.It("bool should be formatted with true or false", func() {
			transform := &JavascriptTransform{}
			ret := transform.ToString(true)
			g.Assert(ret).Equal("true")
		})
		g.It("a map should correctly formatted", func() {
			transform := &JavascriptTransform{}

			lemap := map[string]interface{}{
				"field1": 1,
				"field2": "2",
			}

			ret := transform.ToString(lemap)
			g.Assert(ret).Equal("map[field1:1 field2:2]")

		})
		g.It("an entity should be formatted correctly", func() {
			transform := &JavascriptTransform{}

			e := transform.NewEntity()
			e.ID = "ns1:1"
			e.InternalID = 1
			e.Recorded = 2
			e.Properties = map[string]interface{}{
				"field1": "hello",
			}
			ret := transform.ToString(e)

			g.Assert(ret).Equal("&{ns1:1 1 2 false map[] map[field1:hello]}")
		})
	})
}

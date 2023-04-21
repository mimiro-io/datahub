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

package content

import (
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/mimiro-io/datahub/internal"

	"github.com/franela/goblin"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

func TestContent(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The content API", func() {
		var store *server.Store
		var contentConfig *Config
		var e *conf.Env
		testCnt := 0
		js := ` {
		  "id": "test-import-content",
		  "data": {
			  "databaseServer" : "example",
			  "baseUri" : "https://servers.example.io/",
			  "baseNamespace": "http://server.example.io"
		} } `
		g.BeforeEach(func() {
			testCnt = testCnt + 1
			e = &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: "./test_add_content_" + strconv.Itoa(testCnt),
			}
			err := os.RemoveAll(e.StoreLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + e.StoreLocation)
			lc := fxtest.NewLifecycle(internal.FxTestLog(t, false))
			sc := &statsd.NoOpClient{}
			store = server.NewStore(lc, e, sc)
			lc.RequireStart()
			contentConfig = NewContent(e, store, sc)
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(e.StoreLocation)
		})
		g.It("Should store added content so that it can be read back again", func() {
			content := &Content{}
			err := json.Unmarshal([]byte(js), &content)
			g.Assert(err).IsNil()

			err = contentConfig.AddContent("test-import-content", content)
			g.Assert(err).IsNil()

			content2 := &Content{}
			err = store.GetObject(server.ContentIndex, "test-import-content", content2)
			g.Assert(err).IsNil()

			g.Assert(content2.ID).Eql("test-import-content")
		})

		g.It("Should update content", func() {
			content := &Content{}
			err := json.Unmarshal([]byte(js), content)
			g.Assert(err).IsNil()

			// save new content
			err = contentConfig.AddContent("test-import-content", content)
			g.Assert(err).IsNil()
			g.Assert(content.Data["baseUri"]).Eql("https://servers.example.io/")

			// update content
			content.Data["baseUri"] = "http://data.mimiro.io/test/"
			err = contentConfig.UpdateContent("test-import-content", content)
			g.Assert(err).IsNil()

			// read it back
			content2 := &Content{}
			err = store.GetObject(server.ContentIndex, "test-import-content", content2)
			g.Assert(err).IsNil()
			g.Assert(content2.ID).Eql(content.ID)
			g.Assert(content2.Data["baseUri"]).Eql("http://data.mimiro.io/test/")
		})

		g.It("Should list stored contents", func() {
			content := &Content{}
			err := json.Unmarshal([]byte(js), content)
			g.Assert(err).IsNil()

			err = contentConfig.AddContent("test-import-content", content)
			g.Assert(err).IsNil()

			content2 := &Content{}
			err = json.Unmarshal([]byte(js), content2)
			g.Assert(err).IsNil()

			content2.ID = "test-2"
			err = contentConfig.AddContent("test2", content2)
			g.Assert(err).IsNil()

			res, err := contentConfig.ListContents()
			g.Assert(err).IsNil()
			g.Assert(len(res)).Eql(2)
		})
	})
}

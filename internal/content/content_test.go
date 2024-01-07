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

	"github.com/DataDog/datadog-go/v5/statsd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

func TestContent(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Content Suite")
}

var _ = Describe("The content API", func() {
	var store *server.Store
	var contentConfig *Service
	var e *conf.Config
	testCnt := 0
	js := ` {
		  "id": "test-import-content",
		  "data": {
			  "databaseServer" : "example",
			  "baseUri" : "https://servers.example.io/",
			  "baseNamespace": "http://server.example.io"
		} } `
	BeforeEach(func() {
		testCnt = testCnt + 1
		e = &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: "./test_add_content_" + strconv.Itoa(testCnt),
		}
		err := os.RemoveAll(e.StoreLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+e.StoreLocation)
		//		lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))
		sc := &statsd.NoOpClient{}
		store = server.NewStore(e, sc)
		//		lc.RequireStart()
		contentConfig = NewContentService(e, store, sc)
	})
	AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(e.StoreLocation)
	})
	It("Should store added content so that it can be read back again", func() {
		content := &Content{}
		err := json.Unmarshal([]byte(js), &content)
		Expect(err).To(BeNil())

		err = contentConfig.AddContent("test-import-content", content)
		Expect(err).To(BeNil())

		content2 := &Content{}
		err = store.GetObject(server.ContentIndex, "test-import-content", content2)
		Expect(err).To(BeNil())

		Expect(content2.ID).To(Equal("test-import-content"))
	})

	It("Should update content", func() {
		content := &Content{}
		err := json.Unmarshal([]byte(js), content)
		Expect(err).To(BeNil())

		// save new content
		err = contentConfig.AddContent("test-import-content", content)
		Expect(err).To(BeNil())
		Expect(content.Data["baseUri"]).To(Equal("https://servers.example.io/"))

		// update content
		content.Data["baseUri"] = "http://data.mimiro.io/test/"
		err = contentConfig.UpdateContent("test-import-content", content)
		Expect(err).To(BeNil())

		// read it back
		content2 := &Content{}
		err = store.GetObject(server.ContentIndex, "test-import-content", content2)
		Expect(err).To(BeNil())
		Expect(content2.ID).To(Equal(content.ID))
		Expect(content2.Data["baseUri"]).To(Equal("http://data.mimiro.io/test/"))
	})

	It("Should list stored contents", func() {
		content := &Content{}
		err := json.Unmarshal([]byte(js), content)
		Expect(err).To(BeNil())

		err = contentConfig.AddContent("test-import-content", content)
		Expect(err).To(BeNil())

		content2 := &Content{}
		err = json.Unmarshal([]byte(js), content2)
		Expect(err).To(BeNil())

		content2.ID = "test-2"
		err = contentConfig.AddContent("test2", content2)
		Expect(err).To(BeNil())

		res, err := contentConfig.ListContents()
		Expect(err).To(BeNil())
		Expect(len(res)).To(Equal(2))
	})
})

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
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/franela/goblin"
)

type testEnv struct {
	store                  *Store
	peopleNamespacePrefix  string
	companyNamespacePrefix string
	ds                     *Dataset
	dsm                    *DsManager
	storeLocation          string
}

func TestDataset(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("A Dataset", func() {
		testCnt := 0
		var storeLocation string
		var env *testEnv
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_dataset_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)

			// create store
			e := &conf.Env{Logger: zap.NewNop().Sugar(), StoreLocation: storeLocation}

			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(t)
			s := NewStore(lc, e, &statsd.NoOpClient{})
			dsm := NewDsManager(lc, e, s, NoOpBus())
			err = lc.Start(context.Background())
			os.Stderr = oldErr
			g.Assert(err).IsNil()

			// namespaces
			peopleNamespacePrefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/")
			companyNamespacePrefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/company/")

			// create dataset
			peopleDataset, err := dsm.CreateDataset("people", nil)
			env = &testEnv{s,
				peopleNamespacePrefix,
				companyNamespacePrefix,
				peopleDataset,
				dsm,
				storeLocation}
		})
		g.AfterEach(func() {
			_ = env.store.Close()
			_ = os.RemoveAll(storeLocation)
		})
		g.It("XXX Should accept both single strings and string arrays as refs values", func() {
			//add a first person with all new refs
			person := NewEntity(env.peopleNamespacePrefix+":person-1", 1)
			person.Properties[env.peopleNamespacePrefix+":Name"] = "person 1"
			person.References[env.peopleNamespacePrefix+":worksfor"] = env.companyNamespacePrefix + ":company-3"
			person.References[env.peopleNamespacePrefix+":workedfor"] = []string{
				env.companyNamespacePrefix + ":company-2",
				env.companyNamespacePrefix + ":company-1",
			}
			err := env.ds.StoreEntities([]*Entity{person})
			g.Assert(err).IsNil("Expected entity to be stored without error")

			// query
			queryIds := []string{"http://data.mimiro.io/people/person-1"}
			result, err := env.store.GetManyRelatedEntities(queryIds, "*", false, []string{})
			g.Assert(err).IsNil("Expected query to succeed")

			var company2Seen, company1Seen bool
			for _, r := range result {
				refEntityId := r[2].(*Entity).ID
				relation := r[1].(string)
				if strings.Contains(relation, "worksfor") {
					g.Assert(refEntityId == "ns4:company-3").IsTrue("worksfor should be company-3")
				}
				if strings.Contains(relation, "workedfor") {
					company1Seen = company1Seen || strings.Contains(refEntityId, "company-1")
					company2Seen = company2Seen || strings.Contains(refEntityId, "company-2")
				}
			}
			g.Assert(company1Seen).IsTrue("company-1 was not observed in relations")
			g.Assert(company2Seen).IsTrue("company-2 was not observed in relations")

			//add the same person with updated working relations, thus covering the "not new" branch of code
			person = NewEntity(env.peopleNamespacePrefix+":person-"+strconv.Itoa(1), 0)
			person.Properties[env.peopleNamespacePrefix+":Name"] = "person " + strconv.Itoa(2)
			person.References[env.peopleNamespacePrefix+":worksfor"] = env.companyNamespacePrefix + ":company-4"
			person.References[env.peopleNamespacePrefix+":workedfor"] = []string{
				env.companyNamespacePrefix + ":company-3",
				env.companyNamespacePrefix + ":company-2",
				env.companyNamespacePrefix + ":company-1",
			}

			err = env.ds.StoreEntities([]*Entity{person})
			g.Assert(err).IsNil("Expected entity to be stored without error")

			// query
			result, err = env.store.GetManyRelatedEntities(queryIds, "*", false, []string{})
			g.Assert(err).IsNil("Expected query to succeed")

			company3Seen := false
			company2Seen = false
			company1Seen = false
			for _, r := range result {
				refEntityId := r[2].(*Entity).ID
				relation := r[1].(string)
				if strings.Contains(relation, "worksfor") {
					g.Assert(refEntityId == "ns4:company-4").IsTrue("worksfor should be company-4")
				}
				if strings.Contains(relation, "workedfor") {
					company1Seen = company1Seen || strings.Contains(refEntityId, "company-1")
					company2Seen = company2Seen || strings.Contains(refEntityId, "company-2")
					company3Seen = company3Seen || strings.Contains(refEntityId, "company-3")
				}
			}
			g.Assert(company1Seen).IsTrue("company-1 was not observed in relations")
			g.Assert(company2Seen).IsTrue("company-2 was not observed in relations")
			g.Assert(company3Seen).IsTrue("company-3 was not observed in relations")
		})

		g.It("Should use it's publicNamespaces property for context", func() {
			//first part: test that we get global namespace if nothing is overridden
			_, _ = env.store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/internal/secret/finance")
			_, _ = env.store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/profile/")
			context := env.ds.GetContext()
			g.Assert(len(context.Namespaces)).Eql(7, "There should have been 7 namespaces in the context "+
				"since the dataset does not have publicNamespaces declared.")

			//second part: inject publicNamespaces setting for people ds,
			//and test that it is applied when retrieving context
			coreDsInterface, _ := env.store.datasets.Load("core.Dataset")
			coreDs := coreDsInterface.(*Dataset)
			res, _ := coreDs.GetEntities("", 10)
			for _, e := range res.Entities {
				if e.ID == "ns0:people" {
					nsi, _ := env.store.NamespaceManager.GetDatasetNamespaceInfo()
					e.Properties[nsi.PublicNamespacesKey] = []interface{}{
						"http://data.mimiro.io/people/",
						"http://data.mimiro.io/company/",
					}
					g.Assert(e.Properties[nsi.ItemsKey]).Eql(0.0, "item count should be 0 in core.Dataset")
					err := coreDs.StoreEntities([]*Entity{e})
					g.Assert(err).IsNil()
				}
			}
			context = env.ds.GetContext()
			g.Assert(len(context.Namespaces)).Eql(2, "Expected only 2 namespaces now, since we have"+
				" added a publicNamespaces override")

			//make sure we did not remove the items property when updating publicNamespaces
			res, _ = coreDs.GetEntities("", 10)
			for _, e := range res.Entities {
				if e.ID == "ns0:people" {
					nsi, _ := env.store.NamespaceManager.GetDatasetNamespaceInfo()
					g.Assert(e.Properties[nsi.ItemsKey]).Eql(0.0, "Expected itemscount to be unchanged")
				}
			}

			// add an entity to people, so that the itemcount is updated
			_ = env.ds.StoreEntities([]*Entity{
				NewEntityFromMap(map[string]interface{}{
					"id":    env.peopleNamespacePrefix + ":person-1",
					"props": map[string]interface{}{env.peopleNamespacePrefix + ":Name": "Lisa"},
					"refs":  map[string]interface{}{}})})
			res, _ = coreDs.GetEntities("", 10)

			// verify that the itemcount is updated
			for _, e := range res.Entities {
				if e.ID == "ns0:people" {
					nsi, _ := env.store.NamespaceManager.GetDatasetNamespaceInfo()
					g.Assert(e.Properties[nsi.ItemsKey]).Eql(1.0, "Expected itemscount to be updated")
				}
			}

			// and verify that the context is still correct.
			context = env.ds.GetContext()
			g.Assert(len(context.Namespaces)).Eql(2, "Expected still only 2 namespaces, since we have"+
				" added a publicNamespaces override")
		})
	})
}

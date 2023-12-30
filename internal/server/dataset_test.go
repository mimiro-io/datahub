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
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

type testEnv struct {
	store                  *Store
	peopleNamespacePrefix  string
	companyNamespacePrefix string
	ds                     *Dataset
	dsm                    *DsManager
	storeLocation          string
}

var _ = ginkgo.Describe("A Dataset", func() {
	testCnt := 0
	var storeLocation string
	var env *testEnv
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_dataset_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)

		// create store
		e := &conf.Env{Logger: zap.NewNop().Sugar(), StoreLocation: storeLocation}
		// lc := fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		s := NewStore(e, &statsd.NoOpClient{})
		dsm := NewDsManager(e, s, NoOpBus())
		// err = lc.Start(context.Background())
		// Expect(err).To(BeNil())

		// namespaces
		peopleNamespacePrefix, _ := s.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/people/",
		)
		companyNamespacePrefix, _ := s.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/company/",
		)

		// create dataset
		peopleDataset, _ := dsm.CreateDataset("people", nil)
		env = &testEnv{
			s,
			peopleNamespacePrefix,
			companyNamespacePrefix,
			peopleDataset,
			dsm,
			storeLocation,
		}
	})
	ginkgo.AfterEach(func() {
		_ = env.store.Close()
		_ = os.RemoveAll(storeLocation)
	})
	ginkgo.It("Should accept both single strings and string arrays as refs values", func() {
		// add a first person with all new refs
		person := NewEntity(env.peopleNamespacePrefix+":person-1", 1)
		person.Properties[env.peopleNamespacePrefix+":Name"] = "person 1"
		person.References[env.peopleNamespacePrefix+":worksfor"] = env.companyNamespacePrefix + ":company-3"
		person.References[env.peopleNamespacePrefix+":workedfor"] = []string{
			env.companyNamespacePrefix + ":company-2",
			env.companyNamespacePrefix + ":company-1",
		}
		err := env.ds.StoreEntities([]*Entity{person})
		Expect(err).To(BeNil(), "Expected entity to be stored without error")
		// query
		queryIds := []string{"http://data.mimiro.io/people/person-1"}
		result, err := env.store.GetManyRelatedEntities(queryIds, "*", false, []string{}, true)
		Expect(err).To(BeNil(), "Expected query to succeed")

		var company2Seen, company1Seen bool
		for _, r := range result {
			refEntityID := r[2].(*Entity).ID
			relation := r[1].(string)
			if strings.Contains(relation, "worksfor") {
				Expect(refEntityID == "ns4:company-3").To(BeTrue(), "worksfor should be company-3")
			}
			if strings.Contains(relation, "workedfor") {
				company1Seen = company1Seen || strings.Contains(refEntityID, "company-1")
				company2Seen = company2Seen || strings.Contains(refEntityID, "company-2")
			}
		}
		Expect(company1Seen).To(BeTrue(), "company-1 was not observed in relations")
		Expect(company2Seen).To(BeTrue(), "company-2 was not observed in relations")

		// add the same person with updated working relations, thus covering the "not new" branch of code
		person = NewEntity(env.peopleNamespacePrefix+":person-"+strconv.Itoa(1), 0)
		person.Properties[env.peopleNamespacePrefix+":Name"] = "person " + strconv.Itoa(2)
		person.References[env.peopleNamespacePrefix+":worksfor"] = env.companyNamespacePrefix + ":company-4"
		person.References[env.peopleNamespacePrefix+":workedfor"] = []string{
			env.companyNamespacePrefix + ":company-3",
			env.companyNamespacePrefix + ":company-2",
			env.companyNamespacePrefix + ":company-1",
		}

		err = env.ds.StoreEntities([]*Entity{person})
		Expect(err).To(BeNil(), "Expected entity to be stored without error")

		// query
		result, err = env.store.GetManyRelatedEntities(queryIds, "*", false, []string{}, true)
		Expect(err).To(BeNil(), "Expected query to succeed")

		company3Seen := false
		company2Seen = false
		company1Seen = false
		for _, r := range result {
			refEntityID := r[2].(*Entity).ID
			relation := r[1].(string)
			if strings.Contains(relation, "worksfor") {
				Expect(refEntityID == "ns4:company-4").To(BeTrue(), "worksfor should be company-4")
			}
			if strings.Contains(relation, "workedfor") {
				company1Seen = company1Seen || strings.Contains(refEntityID, "company-1")
				company2Seen = company2Seen || strings.Contains(refEntityID, "company-2")
				company3Seen = company3Seen || strings.Contains(refEntityID, "company-3")
			}
		}
		Expect(company1Seen).To(BeTrue(), "company-1 was not observed in relations")
		Expect(company2Seen).To(BeTrue(), "company-2 was not observed in relations")
		Expect(company3Seen).To(BeTrue(), "company-3 was not observed in relations")
	})

	ginkgo.It("Should use it's publicNamespaces property for context", func() {
		// first part: test that we get global namespace if nothing is overridden
		_, _ = env.store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/internal/secret/finance",
		)
		_, _ = env.store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/people/profile/")
		context := env.ds.GetContext()
		Expect(len(context.Namespaces)).To(Equal(7), "There should have been 7 namespaces in the context "+
			"since the dataset does not have publicNamespaces declared.")

		// second part: inject publicNamespaces setting for people ds,
		// and test that it is applied when retrieving context
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
				Expect(e.Properties[nsi.ItemsKey]).To(Equal(0.0), "item count should be 0 in core.Dataset")
				err := coreDs.StoreEntities([]*Entity{e})
				Expect(err).To(BeNil())
			}
		}
		context = env.ds.GetContext()
		Expect(len(context.Namespaces)).To(Equal(2), "Expected only 2 namespaces now, since we have"+
			" added a publicNamespaces override")

		// make sure we did not remove the items property when updating publicNamespaces
		res, _ = coreDs.GetEntities("", 10)
		for _, e := range res.Entities {
			if e.ID == "ns0:people" {
				nsi, _ := env.store.NamespaceManager.GetDatasetNamespaceInfo()
				Expect(e.Properties[nsi.ItemsKey]).To(Equal(0.0), "Expected itemscount to be unchanged")
			}
		}

		// add an entity to people, so that the itemcount is updated
		_ = env.ds.StoreEntities([]*Entity{
			NewEntityFromMap(map[string]interface{}{
				"id":    env.peopleNamespacePrefix + ":person-1",
				"props": map[string]interface{}{env.peopleNamespacePrefix + ":Name": "Lisa"},
				"refs":  map[string]interface{}{},
			}),
		})
		res, _ = coreDs.GetEntities("", 10)

		// verify that the itemcount is updated
		for _, e := range res.Entities {
			if e.ID == "ns0:people" {
				nsi, _ := env.store.NamespaceManager.GetDatasetNamespaceInfo()
				Expect(e.Properties[nsi.ItemsKey]).To(Equal(1.0), "Expected itemscount to be updated")
			}
		}

		// and verify that the context is still correct.
		context = env.ds.GetContext()
		Expect(len(context.Namespaces)).To(Equal(2), "Expected still only 2 namespaces, since we have"+
			" added a publicNamespaces override")
	})
})

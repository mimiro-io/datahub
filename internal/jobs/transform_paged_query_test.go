package jobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/DataDog/datadog-go/v5/statsd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

var _ = Describe("PagedQuery in transforms", func() {
	testCnt := 0
	var dsm *server.DsManager
	var store *server.Store
	var storeLocation string
	BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_transform_pagedquery_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)

		e := &conf.Env{Logger: logger, StoreLocation: storeLocation}

		lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))
		store = server.NewStore(lc, e, &statsd.NoOpClient{})
		dsm = server.NewDsManager(lc, e, store, server.NoOpBus())
		err = lc.Start(context.Background())
		if err != nil {
			Fail(err.Error())
		}
	})
	AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	It("Should page through all query pages in QueryForEach", func() {
		// setup data
		pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
			{id: 1, friends: []int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
			{id: 21, friends: []int{22, 23, 24, 25}},
			{id: 200, friends: []int{1}},
			{id: 201, friends: []int{1}},
			{id: 202, friends: []int{1}},
			{id: 203, friends: []int{1}},
			{id: 204, friends: []int{1}},
			{id: 205, friends: []int{1}},
		}))

		// setup test transform
		js := ` function transform_entities(entities) {
						const p = GetNamespacePrefix("http://data.mimiro.io/people/")
						let res = NewEntity()
						let pageCnt=0
						let entityCnt = 0
						for (e of entities) {
							let cb = function(batch) {
								pageCnt += 1
								for (item of batch) {
									entityCnt += 1
									SetProperty(res, p, "i-"+pageCnt+"-"+entityCnt, item)
								}
								return true
							}
							let r = PagedQuery([GetId(e), p+":person-2"], "*", false, [], 7, cb)
						}
						SetProperty(res, p,"pageCnt", pageCnt)
						SetProperty(res, p, "entityCnt", entityCnt)
						return [res];
					}`

		f := base64.StdEncoding.EncodeToString([]byte(js))
		transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), f, store, dsm)
		Expect(err).To(BeNil(), "Expected transform runner to be created without error")

		// run transform
		entities := []*server.Entity{{ID: pref + ":person-1"}, {ID: pref + ":person-21"}}
		r, err := transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
		Expect(err).To(BeNil())
		Expect(len(r)).To(Equal(1))
		e := r[0]
		Expect(len(e.Properties)).To(Equal(25))
		// first page 7 items, 2nd page 7 items, 3rd page 5 items (rest of first startUri), 4th page 4 items (all of 2nd startUri)
		Expect(e.GetProperty(pref + ":pageCnt")).To(BeEquivalentTo(4))
		Expect(e.GetProperty(pref + ":entityCnt")).To(BeEquivalentTo(23))
	})

	It("Should stop paging in QueryForEach when callback returns false", func() {
		// setup data
		pref := persist("friends", store, dsm, buildTestBatch(store, []testPerson{
			{id: 1, friends: []int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
			{id: 20, family: []int{21}},
			{id: 21, friends: []int{22, 23, 24, 25}},
			{id: 200, friends: []int{1}},
			{id: 201, friends: []int{1}},
			{id: 202, friends: []int{1}},
			{id: 203, friends: []int{1}},
			{id: 204, friends: []int{1}},
			{id: 205, friends: []int{1}},
		}))

		// setup test transform
		js := ` function transform_entities(entities) {
						const p = GetNamespacePrefix("http://data.mimiro.io/people/")
						let res = NewEntity()
						let pageCnt=0
						let entityCnt = 0
						for (e of entities) {
							let cb = function(batch) {
								pageCnt += 1
								for (item of batch) {
									entityCnt += 1
									SetProperty(res, p, "i-"+pageCnt+"-"+entityCnt, item)
								}
								return false
							}
							let r = PagedQuery([GetId(e), p+":person-2"], "*", false, [], 7, cb)
							break // hop out after first entity
						}
						SetProperty(res, p,"pageCnt", pageCnt)
						SetProperty(res, p, "entityCnt", entityCnt)
						return [res];
					}`

		f := base64.StdEncoding.EncodeToString([]byte(js))
		transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), f, store, dsm)
		Expect(err).To(BeNil(), "Expected transform runner to be created without error")

		// run transform
		entities := []*server.Entity{{ID: pref + ":person-1"}, {ID: pref + ":person-21"}}
		r, err := transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities, "")
		Expect(err).To(BeNil())
		Expect(len(r)).To(Equal(1))
		e := r[0]
		Expect(len(e.Properties)).To(Equal(9)) // 7 items plus 2 counters
		// first page 7 items
		Expect(e.GetProperty(pref + ":pageCnt")).To(BeEquivalentTo(1))
		Expect(e.GetProperty(pref + ":entityCnt")).To(BeEquivalentTo(7))
		Expect(e.GetProperty(pref + ":i-1-1").(server.RelatedEntityResult).RelatedEntity.ID).To(Equal(pref + ":person-20"))
		Expect(e.GetProperty(pref+":i-1-1").(server.RelatedEntityResult).RelatedEntity.Recorded).NotTo(BeZero(), "person-20 should be existing entity")
		Expect(e.GetProperty(pref+":i-1-2").(server.RelatedEntityResult).RelatedEntity.Recorded).To(BeZero(), "other hits are open graph hits, not existing entities")
	})

})

func persist(dsName string, store *server.Store, dsm *server.DsManager, b []*server.Entity) string {
	friendsDS, _ := dsm.CreateDataset(dsName, nil)
	err := friendsDS.StoreEntities(b)
	if err != nil {
		panic(err)
	}
	peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
		"http://data.mimiro.io/people/",
	)
	return peopleNamespacePrefix
}

type testPerson struct {
	id      int
	friends []int
	family  []int
	deleted bool
}

func buildTestBatch(store *server.Store, input []testPerson) []*server.Entity {
	peopleNamespacePrefix, _ := store.NamespaceManager.AssertPrefixMappingForExpansion(
		"http://data.mimiro.io/people/",
	)
	result := make([]*server.Entity, 0, len(input))
	for _, p := range input {
		e := server.NewEntity(fmt.Sprintf(peopleNamespacePrefix+":person-%v", p.id), 0)
		e.Properties[peopleNamespacePrefix+":Name"] = fmt.Sprintf("Person %v", p.id)
		var friends []string
		for _, f := range p.friends {
			friends = append(friends, fmt.Sprintf("%v:person-%v", peopleNamespacePrefix, f))
		}
		if len(friends) > 0 {
			e.References[peopleNamespacePrefix+":Friend"] = friends
		}
		var family []string
		for _, f := range p.family {
			family = append(family, fmt.Sprintf("%v:person-%v", peopleNamespacePrefix, f))
		}
		if len(family) > 0 {
			e.References[peopleNamespacePrefix+":Family"] = family
		}
		e.IsDeleted = p.deleted
		result = append(result, e)
	}
	return result
}

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
						let cnt=0
						for (e of entities) {
							cnt++
							let cb = function(batch) {
								pageCnt += 1
								for (item of batch) {
									entityCnt += 1
									SetProperty(res, p, "i-"+pageCnt+"-"+entityCnt, item)
								}
								return true
							}
							let r = PagedQuery({
								StartURIs:[GetId(e), p+":person-2"],
								Via:"*",
								Inverse: false,
								Datasets: []
							}, 7, cb)
							SetProperty(res, p, "r-"+cnt, "cont:"+r)
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
		// first page 7 items, 2nd page 7 items, 3rd page 5 items (rest of first startUri), 4th page 4 items (all of 2nd startUri)
		Expect(len(e.Properties)).To(Equal(27))
		Expect(e.GetProperty(pref + ":pageCnt")).To(BeEquivalentTo(4))
		Expect(e.GetProperty(pref + ":entityCnt")).To(BeEquivalentTo(23))
		Expect(e.GetProperty(pref + ":r-1")).To(Equal("cont:"))
		Expect(e.GetProperty(pref + ":r-2")).To(Equal("cont:"))
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
							let r = PagedQuery({
								StartURIs:[GetId(e), p+":person-21"],
								Via:"*"
							}, 7, cb);
							SetProperty(res, p, "r", r)
							let r2 = PagedQuery({ Continuations: r }, 5, cb);
							SetProperty(res, p, "r2", r2)
							let r3 = PagedQuery({ Continuations: r2 }, 9, function(batch){ return false });
							SetProperty(res, p, "r3", r3)
							let r4 = PagedQuery({ Continuations: r3 }, 10, cb);
							SetProperty(res, p, "r4", r4)
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
		// 20 consists of
		//   - 7 items from first query (r),
		//   - 5 from continued query (r2)
		//   - none from r3 (skipping 9 exhausts first startURI (7 were remaining) and skips first 2 hits for 2nd startUri)
		//   - 2 from last query, 2 remaining relations of 2nd startURI (r4)
		//   - plus 2 counters: pageCnt and entityCnt
		//   - plus 4 token states (r,r2,r3,r4)
		Expect(len(e.Properties)).To(Equal(20))
		// 7 items first query, 5 from continued query, skipping 8 exhausts first startURI and skips first hit for 2nd startUri, last query adds plus 2 counters, plus 4 token states
		// first page 7 items
		Expect(e.GetProperty(pref+":pageCnt")).To(BeEquivalentTo(3), "3 queries ran cb")
		Expect(e.GetProperty(pref+":entityCnt")).To(BeEquivalentTo(14), "12 first startURI, 2 from 2nd startURI")
		Expect(e.GetProperty(pref + ":r")).NotTo(BeZero())
		Expect(e.GetProperty(pref + ":r2")).NotTo(BeZero())
		Expect(e.GetProperty(pref + ":r3")).NotTo(BeZero())
		Expect(e.GetProperty(pref + ":r4")).To(BeZero())
		Expect(
			e.GetProperty(pref + ":i-1-1").(server.RelatedEntityResult).RelatedEntity.ID,
		).To(Equal(pref + ":person-20"))
		Expect(
			e.GetProperty(pref+":i-1-1").(server.RelatedEntityResult).RelatedEntity.Recorded,
		).NotTo(BeZero(), "person-20 should be existing entity")
		Expect(
			e.GetProperty(pref+":i-1-2").(server.RelatedEntityResult).RelatedEntity.Recorded,
		).To(BeZero(), "other hits are open graph hits, not existing entities")
		Expect(e.GetProperty(pref + ":i-1-3")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-1-4")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-1-5")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-1-6")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-1-7")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-1-8")).To(BeNil())
		Expect(e.GetProperty(pref + ":i-2-8")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-2-9")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-2-10")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-2-11")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-2-12")).NotTo(BeNil())
		Expect(
			e.GetProperty(pref + ":i-2-12").(server.RelatedEntityResult).RelatedEntity.ID,
		).To(Equal(pref + ":person-9"))
		Expect(e.GetProperty(pref + ":i-3-13")).NotTo(BeNil())
		Expect(e.GetProperty(pref + ":i-3-14")).NotTo(BeNil())
		Expect(
			e.GetProperty(pref + ":i-3-14").(server.RelatedEntityResult).RelatedEntity.ID,
		).To(Equal(pref + ":person-22"))
		Expect(e.GetProperty(pref + ":i-3-15")).To(BeNil())
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

package dataset

import (
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/zap"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestLineageBuilder(t *testing.T) {
	var store *server.Store
	var dsm *server.DsManager
	log := zap.NewNop().Sugar()
	testCnt := 0
	setup := func() func() {
		storeLocation := "./test_service_lineage_" + strconv.Itoa(testCnt)
		os.RemoveAll(storeLocation)
		env := &conf.Config{Logger: zap.NewNop().Sugar(), StoreLocation: storeLocation}
		store = server.NewStore(env, &statsd.NoOpClient{})
		dsm = server.NewDsManager(env, store, server.NoOpBus())
		return func() {
			store.Close()
			os.RemoveAll(storeLocation)
		}
	}

	mkJob := func(id, source, sink string, transformInstructions ...[]string) {
		job := &jobs.JobConfiguration{
			ID:    id,
			Title: id,
		}
		if strings.HasPrefix(source, "http") {
			job.Source = map[string]interface{}{"Type": "HttpDatasetSource", "Url": source}
		} else if strings.Contains(source, "+") {
			names := strings.Split(source, "+")
			job.Source = map[string]interface{}{"Type": "UnionDatasetSource", "DatasetSources": []any{
				map[string]interface{}{"Type": "DatasetSource", "Name": names[0]},
			}}
			for _, name := range names[1:] {
				job.Source["DatasetSources"] = append(job.Source["DatasetSources"].([]any), map[string]interface{}{"Type": "DatasetSource", "Name": name})
			}
		} else {
			job.Source = map[string]interface{}{"Type": "DatasetSource", "Name": source}
			dsm.CreateDataset(source, nil)
		}
		if strings.HasPrefix(sink, "http") {
			job.Sink = map[string]interface{}{"Type": "HttpDatasetSink", "Url": sink}
		} else if sink == "" {
			job.Sink = map[string]interface{}{"Type": "DevNullSink"}
		} else {
			job.Sink = map[string]interface{}{"Type": "DatasetSink", "Name": sink}
			dsm.CreateDataset(source, nil)
		}
		if len(transformInstructions) > 0 {
			job.Transform = map[string]interface{}{"Type": "JavascriptTransform", "Code": "console.log('hello')"}
		}
		err := store.StoreObject(server.JobConfigIndex, job.ID, job) // store it for the future
		if err != nil {
			t.Fatalf("CreateJob() error = %v", err)
		}

		if len(transformInstructions) > 0 {
			meta := server.MetaContext{
				QueriedDatasets: make(map[uint32]struct{}),
				TransactionSink: make(map[string]struct{}),
			}
			for _, ti := range transformInstructions {
				if ti[0] == "hop" {
					dsm.CreateDataset(ti[1], nil)
					dsId := dsm.GetDataset(ti[1]).InternalID
					meta.QueriedDatasets[dsId] = struct{}{}
				}
				if ti[0] == "txn" {
					targets := strings.Split(ti[1], ",")
					for _, t := range targets {
						dsm.CreateDataset(t, nil)
						meta.TransactionSink[t] = struct{}{}
					}
				}
			}
			err := store.StoreObject(server.JobMetaIndex, job.ID, &meta)
			if err != nil {
				t.Fatalf("CreateJob() error = %v", err)
			}
		}
	}

	t.Run("ForDataset", func(t *testing.T) {
		t.Run("non existing dataset", func(t *testing.T) {
			defer setup()()
			lb := NewLineageBuilder(store, dsm, log)
			res, err := lb.ForDataset("test")
			if err != nil {
				t.Errorf("ForDataset() error = %v", err)
			}
			if res == nil {
				t.Errorf("ForDataset() res = %v, expect emply Lineage type", res)
			}
			if len(res) != 0 {
				t.Errorf("ForDataset() res = %v, expect emply Lineage type", res)
			}
		})

		t.Run("existing dataset, no jobs", func(t *testing.T) {
			defer setup()()
			lb := NewLineageBuilder(store, dsm, log)
			_, err := dsm.CreateDataset("people", nil)
			if err != nil {
				t.Errorf("CreateDataset() error = %v", err)
			}
			res, err := lb.ForDataset("people")
			if err != nil {
				t.Errorf("ForDataset() error = %v", err)
			}
			if len(res) != 0 {
				t.Errorf("ForDataset() res = %v, expect emply Lineage type", res)
			}
		})

		t.Run("single job", func(t *testing.T) {
			t.Run("copy", func(t *testing.T) {
				t.Run("import from http", func(t *testing.T) {
					defer setup()()
					lb := NewLineageBuilder(store, dsm, log)
					mkJob("test", "http://example.com/people.json", "people")
					res, err := lb.ForDataset("people")
					if err != nil {
						t.Errorf("ForDataset() error = %v", err)
					}

					exp := Lineage{{From: "http://example.com/people.json", To: "people", Type: "copy"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(people) res = %v, expect %v", res, exp)
					}

				})

				t.Run("export to http", func(t *testing.T) {
					defer setup()()
					lb := NewLineageBuilder(store, dsm, log)
					mkJob("test", "people", "http://example.com/people.json")
					res, err := lb.ForDataset("people")
					if err != nil {
						t.Errorf("ForDataset() error = %v", err)
					}

					exp := Lineage{{From: "people", To: "http://example.com/people.json", Type: "copy"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(people) res = %v, expect %v", res, exp)
					}

				})

				t.Run("dataset", func(t *testing.T) {
					defer setup()()
					lb := NewLineageBuilder(store, dsm, log)
					mkJob("test", "people", "persons")
					res, err := lb.ForDataset("people")
					if err != nil {
						t.Errorf("ForDataset() error = %v", err)
					}

					exp := Lineage{{From: "people", To: "persons", Type: "copy"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(people) res = %v, expect %v", res, exp)
					}

					res, err = lb.ForDataset("persons")
					if err != nil {
						t.Errorf("ForDataset() error = %v", err)
					}

					exp = Lineage{{From: "people", To: "persons", Type: "copy"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(persons) res = %v, expect %v", res, exp)
					}

				})
				t.Run("UnionSource", func(t *testing.T) {
					defer setup()()
					lb := NewLineageBuilder(store, dsm, log)
					mkJob("test", "people+cars+cities", "car-owners")
					res, err := lb.ForDataset("people")
					if err != nil {
						t.Errorf("ForDataset() error = %v", err)
					}
					exp := Lineage{{From: "people", To: "car-owners", Type: "copy"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(people) res = %v, expect %v", res, exp)
					}

					res, err = lb.ForDataset("cars")
					exp = Lineage{{From: "cars", To: "car-owners", Type: "copy"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(cars) res = %v, expect %v", res, exp)
					}

					res, err = lb.ForDataset("cities")
					exp = Lineage{{From: "cities", To: "car-owners", Type: "copy"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(cities) res = %v, expect %v", res, exp)
					}

					res, err = lb.ForDataset("car-owners")
					exp = Lineage{
						{From: "cars", To: "car-owners", Type: "copy"},
						{From: "cities", To: "car-owners", Type: "copy"},
						{From: "people", To: "car-owners", Type: "copy"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(car-owners) res = %v, expect %v", res, exp)
					}
				})

			})
			t.Run("transform", func(t *testing.T) {

				t.Run("query", func(t *testing.T) {
					defer setup()()
					lb := NewLineageBuilder(store, dsm, log)
					mkJob("test", "people", "car-owners", []string{"hop", "cars"})
					res, _ := lb.ForDataset("people")
					exp := Lineage{
						{From: "people", To: "car-owners", Type: "transform"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(people) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("cars")
					exp = Lineage{
						{From: "cars", To: "car-owners", Type: "transform-hop"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(cars) res = %v, expect %v", res, exp)
					}
					res, _ = lb.ForDataset("car-owners")
					exp = Lineage{
						{From: "cars", To: "car-owners", Type: "transform-hop"},
						{From: "people", To: "car-owners", Type: "transform"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(car-owners) res = %v, expect %v", res, exp)
					}
				})

				t.Run("transactions", func(t *testing.T) {
					defer setup()()
					lb := NewLineageBuilder(store, dsm, log)
					mkJob("test", "people", "", []string{"txn", "young,adult,senior"})
					res, _ := lb.ForDataset("people")
					exp := Lineage{
						{From: "people", To: "adult", Type: "transform"},
						{From: "people", To: "senior", Type: "transform"},
						{From: "people", To: "young", Type: "transform"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(people) res = %v, expect %v", res, exp)
					}
					res, _ = lb.ForDataset("young")
					exp = Lineage{{From: "people", To: "young", Type: "transform"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(young) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("adult")
					exp = Lineage{{From: "people", To: "adult", Type: "transform"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(adult) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("senior")
					exp = Lineage{{From: "people", To: "senior", Type: "transform"}}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(senior) res = %v, expect %v", res, exp)
					}
				})
				t.Run("transactions, UnionSource, multiple queries", func(t *testing.T) {
					defer setup()()
					lb := NewLineageBuilder(store, dsm, log)
					mkJob("test", "people+persons", "",
						[]string{"hop", "cars"},
						[]string{"hop", "cities"},
						[]string{"txn", "young-car-owners,adult-car-owners,senior-car-owners"})

					res, _ := lb.ForDataset("people")
					exp := Lineage{
						{From: "people", To: "adult-car-owners", Type: "transform"},
						{From: "people", To: "senior-car-owners", Type: "transform"},
						{From: "people", To: "young-car-owners", Type: "transform"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(people) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("persons")
					exp = Lineage{
						{From: "persons", To: "adult-car-owners", Type: "transform"},
						{From: "persons", To: "senior-car-owners", Type: "transform"},
						{From: "persons", To: "young-car-owners", Type: "transform"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(persons) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("cars")
					exp = Lineage{
						{From: "cars", To: "adult-car-owners", Type: "transform-hop"},
						{From: "cars", To: "senior-car-owners", Type: "transform-hop"},
						{From: "cars", To: "young-car-owners", Type: "transform-hop"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(cars) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("cities")
					exp = Lineage{
						{From: "cities", To: "adult-car-owners", Type: "transform-hop"},
						{From: "cities", To: "senior-car-owners", Type: "transform-hop"},
						{From: "cities", To: "young-car-owners", Type: "transform-hop"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(cities) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("young-car-owners")
					exp = Lineage{
						{From: "cars", To: "young-car-owners", Type: "transform-hop"},
						{From: "cities", To: "young-car-owners", Type: "transform-hop"},
						{From: "people", To: "young-car-owners", Type: "transform"},
						{From: "persons", To: "young-car-owners", Type: "transform"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(young-car-owners) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("adult-car-owners")
					exp = Lineage{
						{From: "cars", To: "adult-car-owners", Type: "transform-hop"},
						{From: "cities", To: "adult-car-owners", Type: "transform-hop"},
						{From: "people", To: "adult-car-owners", Type: "transform"},
						{From: "persons", To: "adult-car-owners", Type: "transform"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(adult-car-owners) res = %v, expect %v", res, exp)
					}

					res, _ = lb.ForDataset("senior-car-owners")
					exp = Lineage{
						{From: "cars", To: "senior-car-owners", Type: "transform-hop"},
						{From: "cities", To: "senior-car-owners", Type: "transform-hop"},
						{From: "people", To: "senior-car-owners", Type: "transform"},
						{From: "persons", To: "senior-car-owners", Type: "transform"},
					}
					if !reflect.DeepEqual(res, exp) {
						t.Errorf("ForDataset(senior-car-owners) res = %v, expect %v", res, exp)
					}
				})

			})
		})
		t.Run("many jobs", func(t *testing.T) {
			defer setup()()
			lb := NewLineageBuilder(store, dsm, log)
			mkJob("test", "people+persons", "tax-payers",
				[]string{"hop", "cars"},
				[]string{"hop", "cities"},
				[]string{"txn", "young-car-owners,adult-car-owners,senior-car-owners"})
			mkJob("test2", "vehicles", "cars", []string{"hop", "car-registrations"})
			mkJob("test3", "young-car-owners", "insurance-email-list", []string{"hop", "police-reports"})

			res, _ := lb.ForDataset("people")
			exp := Lineage{
				{From: "people", To: "adult-car-owners", Type: "transform"},
				{From: "people", To: "senior-car-owners", Type: "transform"},
				{From: "people", To: "tax-payers", Type: "transform"},
				{From: "people", To: "young-car-owners", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(people) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("persons")
			exp = Lineage{
				{From: "persons", To: "adult-car-owners", Type: "transform"},
				{From: "persons", To: "senior-car-owners", Type: "transform"},
				{From: "persons", To: "tax-payers", Type: "transform"},
				{From: "persons", To: "young-car-owners", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(persons) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("tax-payers")
			exp = Lineage{
				{From: "cars", To: "tax-payers", Type: "transform-hop"},
				{From: "cities", To: "tax-payers", Type: "transform-hop"},
				{From: "people", To: "tax-payers", Type: "transform"},
				{From: "persons", To: "tax-payers", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(tax-payers) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("cars")
			exp = Lineage{
				{From: "car-registrations", To: "cars", Type: "transform-hop"},
				{From: "cars", To: "adult-car-owners", Type: "transform-hop"},
				{From: "cars", To: "senior-car-owners", Type: "transform-hop"},
				{From: "cars", To: "tax-payers", Type: "transform-hop"},
				{From: "cars", To: "young-car-owners", Type: "transform-hop"},
				{From: "vehicles", To: "cars", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(cars) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("cities")
			exp = Lineage{
				{From: "cities", To: "adult-car-owners", Type: "transform-hop"},
				{From: "cities", To: "senior-car-owners", Type: "transform-hop"},
				{From: "cities", To: "tax-payers", Type: "transform-hop"},
				{From: "cities", To: "young-car-owners", Type: "transform-hop"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(cities) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("young-car-owners")
			exp = Lineage{
				{From: "cars", To: "young-car-owners", Type: "transform-hop"},
				{From: "cities", To: "young-car-owners", Type: "transform-hop"},
				{From: "people", To: "young-car-owners", Type: "transform"},
				{From: "persons", To: "young-car-owners", Type: "transform"},
				{From: "young-car-owners", To: "insurance-email-list", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(young-car-owners) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("adult-car-owners")
			exp = Lineage{
				{From: "cars", To: "adult-car-owners", Type: "transform-hop"},
				{From: "cities", To: "adult-car-owners", Type: "transform-hop"},
				{From: "people", To: "adult-car-owners", Type: "transform"},
				{From: "persons", To: "adult-car-owners", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(adult-car-owners) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("senior-car-owners")
			exp = Lineage{
				{From: "cars", To: "senior-car-owners", Type: "transform-hop"},
				{From: "cities", To: "senior-car-owners", Type: "transform-hop"},
				{From: "people", To: "senior-car-owners", Type: "transform"},
				{From: "persons", To: "senior-car-owners", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(senior-car-owners) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("vehicles")
			exp = Lineage{
				{From: "vehicles", To: "cars", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(vehicles) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("car-registrations")
			exp = Lineage{
				{From: "car-registrations", To: "cars", Type: "transform-hop"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(car-registrations) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("insurance-email-list")
			exp = Lineage{
				{From: "police-reports", To: "insurance-email-list", Type: "transform-hop"},
				{From: "young-car-owners", To: "insurance-email-list", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(insurance-email-list) res = %v, expect %v", res, exp)
			}

			res, _ = lb.ForDataset("police-reports")
			exp = Lineage{
				{From: "police-reports", To: "insurance-email-list", Type: "transform-hop"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForDataset(police-reports) res = %v, expect %v", res, exp)
			}
		})
	})
	t.Run("ForAll", func(t *testing.T) {
		t.Run("many jobs", func(t *testing.T) {
			defer setup()()
			lb := NewLineageBuilder(store, dsm, log)
			mkJob("test", "people+persons", "tax-payers",
				[]string{"hop", "cars"},
				[]string{"hop", "cities"},
				[]string{"txn", "young-car-owners,adult-car-owners,senior-car-owners"})
			mkJob("test2", "vehicles", "cars", []string{"hop", "car-registrations"})
			mkJob("test3", "young-car-owners", "insurance-email-list", []string{"hop", "police-reports"})

			res, err := lb.ForAll()
			if err != nil {
				t.Errorf("ForAll() error = %v", err)
			}
			exp := Lineage{
				{From: "car-registrations", To: "cars", Type: "transform-hop"},
				{From: "cars", To: "adult-car-owners", Type: "transform-hop"},
				{From: "cars", To: "senior-car-owners", Type: "transform-hop"},
				{From: "cars", To: "tax-payers", Type: "transform-hop"},
				{From: "cars", To: "young-car-owners", Type: "transform-hop"},
				{From: "cities", To: "adult-car-owners", Type: "transform-hop"},
				{From: "cities", To: "senior-car-owners", Type: "transform-hop"},
				{From: "cities", To: "tax-payers", Type: "transform-hop"},
				{From: "cities", To: "young-car-owners", Type: "transform-hop"},
				{From: "people", To: "adult-car-owners", Type: "transform"},
				{From: "people", To: "senior-car-owners", Type: "transform"},
				{From: "people", To: "tax-payers", Type: "transform"},
				{From: "people", To: "young-car-owners", Type: "transform"},
				{From: "persons", To: "adult-car-owners", Type: "transform"},
				{From: "persons", To: "senior-car-owners", Type: "transform"},
				{From: "persons", To: "tax-payers", Type: "transform"},
				{From: "persons", To: "young-car-owners", Type: "transform"},
				{From: "police-reports", To: "insurance-email-list", Type: "transform-hop"},
				{From: "vehicles", To: "cars", Type: "transform"},
				{From: "young-car-owners", To: "insurance-email-list", Type: "transform"},
			}
			if !reflect.DeepEqual(res, exp) {
				t.Errorf("ForAll() res = %v, expect %v", res, exp)
			}

		})
	})
}

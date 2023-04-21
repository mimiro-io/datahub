package jobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

// implements QueryResultWriter interface
type TestQueryResultWriter struct {
	Results []interface{}
}

func NewTestQueryResultWriter() *TestQueryResultWriter {
	return &TestQueryResultWriter{Results: make([]interface{}, 0)}
}

func (t *TestQueryResultWriter) WriteObject(result interface{}) error {
	t.Results = append(t.Results, result)
	return nil
}

func TestQuery(test *testing.T) {
	g := goblin.Goblin(test)

	g.Describe("QueryExecution", func() {
		testCnt := 0
		var dsm *server.DsManager
		var store *server.Store
		var storeLocation string
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_store_relations_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean test files in " + storeLocation)
			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}
			lc := fxtest.NewLifecycle(internal.FxTestLog(test, false))
			store = server.NewStore(lc, e, &statsd.NoOpClient{})
			dsm = server.NewDsManager(lc, e, store, server.NoOpBus())

			err = lc.Start(context.Background())
			g.Assert(err).IsNil()
		})
		g.AfterEach(func() {
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("Should support js query that just writes to the response writer", func() {
			// transform js
			js := `
			function do_query() {
				let obj = { "name": "homer" };
				WriteQueryResult(obj);
			}
			`
			queryEncoded := base64.StdEncoding.EncodeToString([]byte(js))

			// create query transform
			transform, _ := NewJavascriptTransform(zap.NewNop().Sugar(), queryEncoded, store, dsm)

			// create test result writer
			resultWriter := NewTestQueryResultWriter()

			// run query
			err := transform.ExecuteQuery(resultWriter)
			if err != nil {
				g.Fail(err)
			}

			// check result
			g.Assert(len(resultWriter.Results)).Equal(1)
			g.Assert(resultWriter.Results[0]).Equal(map[string]interface{}{"name": "homer"})
		})

		g.It("Should support js query that can access dataset changes", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("people", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// transform js
			js := `
			function do_query() {
				let obj = GetDatasetChanges("people", 0, 5);
				let entities = obj.Entities;
				let count = 0;
				for (let i = 0; i < entities.length; i++) {
					count++;
				}

				let result = { "count": count };
				WriteQueryResult(result);
			}
			`

			queryEncoded := base64.StdEncoding.EncodeToString([]byte(js))

			// create query transform
			transform, _ := NewJavascriptTransform(zap.NewNop().Sugar(), queryEncoded, store, dsm)

			// create test result writer
			resultWriter := NewTestQueryResultWriter()

			// run query
			err = transform.ExecuteQuery(resultWriter)
			if err != nil {
				g.Fail(err)
			}

			// check result
			g.Assert(len(resultWriter.Results)).Equal(1)
			g.Assert(resultWriter.Results[0]).Equal(map[string]interface{}{"count": 1})
		})

		g.It("Should support js query that can access dataset changes with continuation token", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("people", nil)

			entities := make([]*server.Entity, 2)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			entity = server.NewEntity("http://data.mimiro.io/people/marge", 0)
			entity.Properties["name"] = "marge"
			entities[1] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// transform js
			js := `
			function do_query() {
				let count = 0;
				let names = [];

				let obj1 = GetDatasetChanges("people", 0, 1);
				let entities1 = obj1.Entities;
				for (let i = 0; i < entities1.length; i++) {
					names.push(entities1[i].ID);
					count++;
				}

				let obj2 = GetDatasetChanges("people", obj1.NextToken, 1);
				let entities2 = obj2.Entities;
				for (let i = 0; i < entities2.length; i++) {
					names.push(entities2[i].ID);
					count++;
				}

				let result = { "count": count, "names": names };
				WriteQueryResult(result);
			}
			`

			queryEncoded := base64.StdEncoding.EncodeToString([]byte(js))

			// create query transform
			transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), queryEncoded, store, dsm)
			if err != nil {
				g.Fail(err)
			}

			// create test result writer
			resultWriter := NewTestQueryResultWriter()

			// run query
			err = transform.ExecuteQuery(resultWriter)
			if err != nil {
				g.Fail(err)
			}

			// check result
			g.Assert(len(resultWriter.Results)).Equal(1)
			g.Assert(resultWriter.Results[0]).
				Equal(map[string]interface{}{"count": 2, "names": []interface{}{"http://data.mimiro.io/people/homer", "http://data.mimiro.io/people/marge"}})
		})
	})
}

package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestFullSync(t *testing.T) {
	g := goblin.Goblin(t)

	var app *fx.App

	location := "./dataset_fullsync_integration_test"
	dsUrl := "http://localhost:24998/datasets/bananas"

	g.Describe("The dataset endpoint", func() {
		g.Before(func() {

			_ = os.Setenv("STORE_LOCATION", location)
			_ = os.Setenv("PROFILE", "test")
			_ = os.Setenv("SERVER_PORT", "24998")
			oldOut := os.Stdout
			oldErr := os.Stderr
			devNull, _ := os.Open("/dev/null")
			os.Stdout = devNull
			os.Stderr = devNull
			app, _ = Start(context.Background())
			os.Stdout = oldOut
			os.Stderr = oldErr
		})
		g.After(func() {
			ctx := context.Background()
			err := app.Stop(ctx)
			g.Assert(err).IsNil()
			err = os.RemoveAll(location)
			g.Assert(err).IsNil()
		})

		g.It("Should create a dataset", func() {
			//create new dataset
			res, err := http.Post(dsUrl, "application/json", strings.NewReader(""))
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)
		})
		g.It("Should accept a single batch of changes", func() {
			//populate dataset
			payload := strings.NewReader(bananasFromTo(1, 10, false))
			res, err := http.Post(dsUrl+"/entities", "application/json", payload)

			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			//read it back
			res, err = http.Get(dsUrl + "/changes")
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			bodyBytes, err := ioutil.ReadAll(res.Body)
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(len(entities)).Eql(12, "expected 10 entities plus @context and @continuation")
		})

		g.It("Should accept multiple overlapping batches of changes", func() {
			//replace 5-10 and add 11-15
			payload := strings.NewReader(bananasFromTo(5, 15, false))
			res, err := http.Post(dsUrl+"/entities", "application/json", payload)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			// replace 10-15 and add 16-20
			payload = strings.NewReader(bananasFromTo(10, 20, false))
			res, err = http.Post(dsUrl+"/entities", "application/json", payload)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			// read it back
			res, err = http.Get(dsUrl + "/changes")
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			_ = res.Body.Close()
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(len(entities)).Eql(22, "expected 20 entities plus @context and @continuation")
		})

		g.It("Should record deleted states", func() {
			payload := strings.NewReader(bananasFromTo(7, 8, true))
			res, err := http.Post(dsUrl+"/entities", "application/json", payload)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)

			// read changes back
			res, err = http.Get(dsUrl + "/changes")
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			_ = res.Body.Close()
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(len(entities)).Eql(24, "expected 20 entities plus 2 deleted-changes plus @context and @continuation")
			g.Assert(entities[7].IsDeleted).IsFalse("original change 7 is still undeleted")
			g.Assert(entities[22].IsDeleted).IsTrue("deleted state for 7  is a new change at end of list")

			// read entities back
			res, err = http.Get(dsUrl + "/entities")
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)
			bodyBytes, _ = ioutil.ReadAll(res.Body)
			_ = res.Body.Close()
			entities = nil
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(len(entities)).Eql(22, "expected 20 entities plus @context and @continuation")
			g.Assert(entities[7].IsDeleted).IsTrue("entity 7 is deleted")
		})

		g.It("Should do deletion detection in a fullsync", func() {
			// only send IDs 4 through 16 in batches as fullsync
			// 1-3 and 17-20 should end up deleted

			// first batch with "start" header
			payload := strings.NewReader(bananasFromTo(4, 8, false))
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, _ = http.DefaultClient.Do(req)
			cancel()

			// 2nd batch
			payload = strings.NewReader(bananasFromTo(9, 12, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, _ = http.DefaultClient.Do(req)
			cancel()

			// last batch with "end" signal
			payload = strings.NewReader(bananasFromTo(13, 16, false))
			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			req, _ = http.NewRequestWithContext(ctx, "POST", dsUrl+"/entities", payload)
			req.Header.Add("universal-data-api-full-sync-id", "42")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			_, _ = http.DefaultClient.Do(req)
			cancel()


			// read changes back
			res, err := http.Get(dsUrl + "/changes")
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			_ = res.Body.Close()
			var entities []*server.Entity
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(len(entities)).Eql(33, "expected 20 entities plus 13 changes and @context and @continuation")
			g.Assert(entities[7].IsDeleted).IsFalse("original change 7 is still undeleted")
			g.Assert(entities[22].IsDeleted).IsTrue("deleted state for 7  is a new change at end of list")

			// read entities back
			res, err = http.Get(dsUrl + "/entities")
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			g.Assert(res.StatusCode).Eql(200)
			bodyBytes, _ = ioutil.ReadAll(res.Body)
			_ = res.Body.Close()
			entities = nil
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(len(entities)).Eql(22, "expected 20 entities plus @context and @continuation")
			//remove context
			entities = entities[1:]
			for i := 0; i < 3; i++ {
				g.Assert(entities[i].IsDeleted).IsTrue("entity was not part of fullsync, should be deleted: ", i)
			}
			for i := 3; i < 16; i++ {
				g.Assert(entities[i].IsDeleted).IsFalse("entity was part of fullsync, should be active: ", i)
			}
			for i := 16; i < 20; i++ {
				g.Assert(entities[i].IsDeleted).IsTrue("entity was not part of fullsync, should be deleted: ", i)
			}
		})

		g.It("should handle fullsync requests with same sync-id in parallel")
		g.It("should abandon fullsync and start new fullsync if new start-signal is sent during sync")
		g.It("should reject fullsync when job writing to same dataset is running?")
		g.It("should not start job writing to same dataset while http fullsync is running?")
	})
}

func bananasFromTo(from, to int, deleted bool) string {
	prefix := `[ { "id" : "@context", "namespaces" : { "_" : "http://example.com" } }, `

	var bananas []string
	for i := from; i <= to; i++ {
		if deleted {
			bananas = append(bananas, fmt.Sprintf(`{ "id" : "%v", "deleted": true }`, i))
		} else {
			bananas = append(bananas, fmt.Sprintf(`{ "id" : "%v" }`, i))
		}
	}

	return prefix + strings.Join(bananas, ",") + "]"
}

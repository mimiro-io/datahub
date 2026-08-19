// Copyright 2026 MIMIRO AS
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

package scheduler

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

// testCleanupTask builds a namespace cleanup task over a store in a temporary location,
// and returns the task together with the store it scans.
func testCleanupTask(t *testing.T, doDelete bool) (schedulable, *server.Store, *server.DsManager) {
	t.Helper()
	e := &conf.Config{
		Logger:        zap.NewNop().Sugar(),
		StoreLocation: t.TempDir(),
	}
	s := server.NewStore(e, &statsd.NoOpClient{})
	t.Cleanup(func() { _ = s.Close() })
	dsm := server.NewDsManager(e, s, server.NoOpBus())

	return NewNamespaceCleaner(e.Logger, server.NewBadgerAccess(s, dsm), doDelete), s, dsm
}

// awaitTask waits for a started task to leave the running state.
func awaitTask(t *testing.T, task schedulable) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if task.State() == TaskStateScheduled {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not finish within 20s, state is %q", task.ID(), task.State())
}

func TestNamespaceCleanupTaskFinishes(t *testing.T) {
	t.Run("Should leave the running state after a completed scan", func(t *testing.T) {
		task, _, _ := testCleanupTask(t, false)

		task.Run()
		awaitTask(t, task)

		// a second run has to be possible, otherwise the weekly schedule is dead
		task.Run()
		awaitTask(t, task)
	})

	t.Run("Should stop without blocking when no scan is running", func(t *testing.T) {
		task, _, _ := testCleanupTask(t, false)

		task.Run()
		awaitTask(t, task)

		stopped := make(chan struct{})
		go func() {
			task.Stop(context.Background())
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(20 * time.Second):
			t.Fatal("Stop did not return within 20s")
		}
	})

	t.Run("Should let the store close directly after Stop, as shutdown does", func(t *testing.T) {
		task, s, dsm := testCleanupTask(t, false)

		// enough entities that the scan is plausibly still running when Stop arrives
		prefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion("http://data.example.io/people/")
		if err != nil {
			t.Fatalf("failed to assert prefix: %v", err)
		}
		ds, err := dsm.CreateDataset("people", nil)
		if err != nil {
			t.Fatalf("failed to create dataset: %v", err)
		}
		entities := make([]*server.Entity, 0, 5000)
		for i := 0; i < 5000; i++ {
			entities = append(entities, server.NewEntityFromMap(map[string]interface{}{
				"id":    prefix + ":person-" + strconv.Itoa(i),
				"props": map[string]interface{}{prefix + ":name": "person " + strconv.Itoa(i)},
				"refs":  map[string]interface{}{},
			}))
		}
		if err := ds.StoreEntities(entities); err != nil {
			t.Fatalf("failed to store entities: %v", err)
		}

		task.Run()
		stopped := make(chan struct{})
		go func() {
			// Stop must wait for the scan to leave the badger stream, so that this
			// close cannot race a running iterator
			task.Stop(context.Background())
			_ = s.Close()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(20 * time.Second):
			t.Fatal("Stop and store close did not finish within 20s")
		}
	})
}

func TestNamespaceCleanupDeletesUnusedPrefixes(t *testing.T) {
	task, s, dsm := testCleanupTask(t, true)

	usedExpansion := "http://data.example.io/people/"
	usedPrefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion(usedExpansion)
	if err != nil {
		t.Fatalf("failed to assert used prefix: %v", err)
	}
	unusedPrefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion("http://data.example.io/orphan/")
	if err != nil {
		t.Fatalf("failed to assert unused prefix: %v", err)
	}

	ds, err := dsm.CreateDataset("people", nil)
	if err != nil {
		t.Fatalf("failed to create dataset: %v", err)
	}
	err = ds.StoreEntities([]*server.Entity{
		server.NewEntityFromMap(map[string]interface{}{
			"id":    usedPrefix + ":homer",
			"props": map[string]interface{}{usedPrefix + ":name": "Homer"},
			"refs":  map[string]interface{}{usedPrefix + ":partner": usedPrefix + ":marge"},
		}),
	})
	if err != nil {
		t.Fatalf("failed to store entities: %v", err)
	}

	task.Run()
	awaitTask(t, task)

	mapping := s.NamespaceManager.GetPrefixToExpansionMap()
	if _, found := mapping[unusedPrefix]; found {
		t.Errorf("unused prefix %s should have been deleted", unusedPrefix)
	}
	if mapping[usedPrefix] != usedExpansion {
		t.Errorf("prefix %s is used by a stored entity and must survive, got %q", usedPrefix, mapping[usedPrefix])
	}
}

func TestExtractPrefix(t *testing.T) {
	cleaner := &NamespaceCleaner{}
	for _, tc := range []struct {
		value string
		want  string
	}{
		{"ns0:homer", "ns0"},
		{"ns12:some:thing", "ns12"},
		{"http://data.example.io/people/homer", ""},
		{"https://data.example.io/people/homer", ""},
		{"urn:isbn:0451450523", ""},
		{"homer", ""},
		{":homer", ""},
		{"ns0:", ""},
		{"", ""},
	} {
		if got := cleaner.extractPrefix(tc.value); got != tc.want {
			t.Errorf("extractPrefix(%q) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

func TestExtractNamespacePrefixesFromJSON(t *testing.T) {
	cleaner := &NamespaceCleaner{}

	t.Run("Should collect prefixes from the id, property keys, and reference keys and values", func(t *testing.T) {
		entity := `{
			"id": "ns0:homer",
			"props": {"ns1:name": "Homer", "ns1:born": "1956-05-12"},
			"refs": {
				"ns2:type": "ns3:Person",
				"ns2:friends": ["ns4:barney", "ns4:lenny"],
				"ns2:employer": "http://data.example.io/company/snpp"
			}
		}`

		got, err := cleaner.extractNamespacePrefixesFromJSON([]byte(entity))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		sort.Strings(got)
		want := []string{"ns0", "ns1", "ns2", "ns3", "ns4"}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	})

	t.Run("Should return an error for data that is not an entity", func(t *testing.T) {
		_, err := cleaner.extractNamespacePrefixesFromJSON([]byte("not json"))
		if err == nil {
			t.Error("expected an error for malformed entity data")
		}
	})
}

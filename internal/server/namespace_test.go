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

package server

import (
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

func testStore(t *testing.T, location string) *Store {
	t.Helper()
	return NewStore(&conf.Config{
		Logger:        zap.NewNop().Sugar(),
		StoreLocation: location,
	}, &statsd.NoOpClient{})
}

func assertPrefix(t *testing.T, s *Store, expansion string) string {
	t.Helper()
	prefix, err := s.NamespaceManager.AssertPrefixMappingForExpansion(expansion)
	if err != nil {
		t.Fatalf("failed to assert prefix for %s: %v", expansion, err)
	}
	return prefix
}

func TestDeleteNamespacePrefix(t *testing.T) {
	t.Run("Should remove both directions of the mapping", func(t *testing.T) {
		s := testStore(t, t.TempDir())
		defer s.Close()

		prefix := assertPrefix(t, s, "http://data.example.io/people/")

		err := s.NamespaceManager.DeleteNamespacePrefix(prefix)
		if err != nil {
			t.Fatalf("failed to delete prefix %s: %v", prefix, err)
		}

		if _, found := s.NamespaceManager.GetPrefixToExpansionMap()[prefix]; found {
			t.Errorf("prefix %s should be gone from the prefix mapping", prefix)
		}
		_, err = s.NamespaceManager.GetPrefixMappingForExpansion("http://data.example.io/people/")
		if err == nil {
			t.Error("expansion should be gone from the expansion mapping")
		}
	})

	t.Run("Should be a no-op for an unknown prefix", func(t *testing.T) {
		s := testStore(t, t.TempDir())
		defer s.Close()

		prefix := assertPrefix(t, s, "http://data.example.io/people/")

		err := s.NamespaceManager.DeleteNamespacePrefix("ns4711")
		if err != nil {
			t.Fatalf("deleting an unknown prefix should not fail: %v", err)
		}
		if len(s.NamespaceManager.GetPrefixToExpansionMap()) != 1 {
			t.Errorf("mapping for %s should be untouched", prefix)
		}
	})
}

func TestNamespacePrefixesAreNotReusedAfterDelete(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	first := assertPrefix(t, s, "http://data.example.io/first/")
	second := assertPrefix(t, s, "http://data.example.io/second/")
	third := assertPrefix(t, s, "http://data.example.io/third/")
	if first != "ns0" || second != "ns1" || third != "ns2" {
		t.Fatalf("expected ns0, ns1, ns2, got %s, %s, %s", first, second, third)
	}

	err := s.NamespaceManager.DeleteNamespacePrefix(second)
	if err != nil {
		t.Fatalf("failed to delete prefix %s: %v", second, err)
	}

	// entities stored earlier still carry ns1 curies, so handing ns1 to another
	// expansion would silently remap them
	fourth := assertPrefix(t, s, "http://data.example.io/fourth/")
	if fourth != "ns3" {
		t.Errorf("expected ns3 for the next expansion, got %s", fourth)
	}

	mapping := s.NamespaceManager.GetPrefixToExpansionMap()
	if mapping[first] != "http://data.example.io/first/" {
		t.Errorf("%s should still expand to the first namespace, got %s", first, mapping[first])
	}
	if mapping[third] != "http://data.example.io/third/" {
		t.Errorf("%s should still expand to the third namespace, got %s", third, mapping[third])
	}
}

func TestNamespacePrefixCounterSurvivesRestart(t *testing.T) {
	location := t.TempDir()

	s := testStore(t, location)
	assertPrefix(t, s, "http://data.example.io/first/")
	second := assertPrefix(t, s, "http://data.example.io/second/")
	assertPrefix(t, s, "http://data.example.io/third/")
	err := s.NamespaceManager.DeleteNamespacePrefix(second)
	if err != nil {
		t.Fatalf("failed to delete prefix %s: %v", second, err)
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	restarted := testStore(t, location)
	defer restarted.Close()

	if got := assertPrefix(t, restarted, "http://data.example.io/fourth/"); got != "ns3" {
		t.Errorf("expected ns3 after restart, got %s", got)
	}
	if got := assertPrefix(t, restarted, "http://data.example.io/fifth/"); got != "ns4" {
		t.Errorf("expected ns4 after restart, got %s", got)
	}
}

func TestNamespacePrefixCounterRecoveredFromStateWithoutCounter(t *testing.T) {
	location := t.TempDir()

	s := testStore(t, location)
	// state written before NextPrefixID existed holds the two mappings only
	legacyState := map[string]interface{}{
		"PrefixToExpansionMapping": map[string]string{
			"ns0": "http://data.example.io/first/",
			"ns1": "http://data.example.io/second/",
			"ns2": "http://data.example.io/third/",
		},
		"ExpansionToPrefixMapping": map[string]string{
			"http://data.example.io/first/":  "ns0",
			"http://data.example.io/second/": "ns1",
			"http://data.example.io/third/":  "ns2",
		},
	}
	err := s.StoreObject(NamespacesIndex, "namespacestate", legacyState)
	if err != nil {
		t.Fatalf("failed to store legacy namespace state: %v", err)
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	restarted := testStore(t, location)
	defer restarted.Close()

	if got := restarted.NamespaceManager.nextPrefixID; got != 3 {
		t.Errorf("expected the counter to recover to 3, got %d", got)
	}
	if got := assertPrefix(t, restarted, "http://data.example.io/fourth/"); got != "ns3" {
		t.Errorf("expected ns3, got %s", got)
	}
}

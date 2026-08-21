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

func TestAssertSpecificPrefixMapping(t *testing.T) {
	t.Run("Should restore a deleted mapping under its exact prefix", func(t *testing.T) {
		s := testStore(t, t.TempDir())
		defer s.Close()

		prefix := assertPrefix(t, s, "http://data.example.io/people/")
		if err := s.NamespaceManager.DeleteNamespacePrefix(prefix); err != nil {
			t.Fatalf("failed to delete prefix: %v", err)
		}

		err := s.NamespaceManager.AssertSpecificPrefixMapping(prefix, "http://data.example.io/people/")
		if err != nil {
			t.Fatalf("failed to restore mapping: %v", err)
		}
		expanded, err := s.NamespaceManager.ExpandCurie(prefix + ":bob")
		if err != nil {
			t.Fatalf("restored prefix must expand again: %v", err)
		}
		if expanded != "http://data.example.io/people/bob" {
			t.Errorf("unexpected expansion %s", expanded)
		}
		roundtrip, err := s.NamespaceManager.GetPrefixMappingForExpansion("http://data.example.io/people/")
		if err != nil || roundtrip != prefix {
			t.Errorf("expected expansion to map back to %s, got %s (%v)", prefix, roundtrip, err)
		}
	})

	t.Run("Should accept the same mapping twice", func(t *testing.T) {
		s := testStore(t, t.TempDir())
		defer s.Close()

		if err := s.NamespaceManager.AssertSpecificPrefixMapping("ns40", "http://data.example.io/a/"); err != nil {
			t.Fatalf("first restore failed: %v", err)
		}
		if err := s.NamespaceManager.AssertSpecificPrefixMapping("ns40", "http://data.example.io/a/"); err != nil {
			t.Errorf("restore must be idempotent, got: %v", err)
		}
	})

	t.Run("Should refuse a prefix that is mapped to a different expansion", func(t *testing.T) {
		s := testStore(t, t.TempDir())
		defer s.Close()

		prefix := assertPrefix(t, s, "http://data.example.io/people/")
		err := s.NamespaceManager.AssertSpecificPrefixMapping(prefix, "http://data.example.io/other/")
		if err == nil {
			t.Error("expected an error when the prefix is taken by another expansion")
		}
	})

	t.Run("Should keep generated prefixes clear of a restored prefix", func(t *testing.T) {
		s := testStore(t, t.TempDir())
		defer s.Close()

		if err := s.NamespaceManager.AssertSpecificPrefixMapping("ns40", "http://data.example.io/a/"); err != nil {
			t.Fatalf("restore failed: %v", err)
		}
		next := assertPrefix(t, s, "http://data.example.io/b/")
		if next != "ns41" {
			t.Errorf("expected the counter to jump past the restored prefix, got %s", next)
		}
	})

	t.Run("Should not take over the write path when the expansion has a newer prefix", func(t *testing.T) {
		s := testStore(t, t.TempDir())
		defer s.Close()

		newer := assertPrefix(t, s, "http://data.example.io/people/")
		if err := s.NamespaceManager.AssertSpecificPrefixMapping("ns99", "http://data.example.io/people/"); err != nil {
			t.Fatalf("restore failed: %v", err)
		}
		writePrefix, err := s.NamespaceManager.GetPrefixMappingForExpansion("http://data.example.io/people/")
		if err != nil || writePrefix != newer {
			t.Errorf("the newer prefix %s must keep the write path, got %s (%v)", newer, writePrefix, err)
		}
		expanded, err := s.NamespaceManager.ExpandCurie("ns99:bob")
		if err != nil || expanded != "http://data.example.io/people/bob" {
			t.Errorf("restored prefix must still resolve, got %s (%v)", expanded, err)
		}
	})

	t.Run("Should survive a restart", func(t *testing.T) {
		location := t.TempDir()
		s := testStore(t, location)

		if err := s.NamespaceManager.AssertSpecificPrefixMapping("ns673", "http://data.example.io/transfers/"); err != nil {
			t.Fatalf("restore failed: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("failed to close store: %v", err)
		}

		restarted := testStore(t, location)
		defer restarted.Close()
		expanded, err := restarted.NamespaceManager.ExpandCurie("ns673:1697017997")
		if err != nil || expanded != "http://data.example.io/transfers/1697017997" {
			t.Errorf("restored mapping must survive a restart, got %s (%v)", expanded, err)
		}
	})
}

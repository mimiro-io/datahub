// Copyright 2025 MIMIRO AS
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

package middlewares

import (
	"slices"
	"testing"

	"go.uber.org/zap"
)

func Test_parse_wildcard(t *testing.T) {
	result := []byte("{\"decision_id\":\"7363d2f4-e9fe-4fee-9d79-c1b5efe27483\",\"result\":{\"*\":true}}")
	ds, err := parseDatasetsFromOpaBody(zap.NewNop().Sugar(), result)

	if err != nil {
		t.Fatalf("should parse : %+v", err)
	}

	if len(ds) != 1 {
		t.Fatalf("should have 1 dataset : %+v", ds)
	}

	if ds[0] != "*" {
		t.Fatalf("should have * dataset : %+v", ds)
	}
}

func Test_parse_slice_datasets(t *testing.T) {
	result := []byte("{\"decision_id\":\"5c78dda3-6389-4fc3-9f01-764f5ef9fe36\",\"result\":[\"TestEvent1\", \"TestEvent2\"]}")
	ds, err := parseDatasetsFromOpaBody(zap.NewNop().Sugar(), result)

	if err != nil {
		t.Fatalf("should parse : %+v", err)
	}
	if len(ds) != 2 {
		t.Fatalf("should have 2 datasets : %+v", ds)
	}
	if !slices.Contains(ds, "TestEvent1") {
		t.Fatalf("should have TestEvent1 dataset : %+v", ds)
	}
	if !slices.Contains(ds, "TestEvent2") {
		t.Fatalf("should have TestEvent2 dataset : %+v", ds)
	}
}

func Test_parse_datasets(t *testing.T) {
	result := []byte("{\"decision_id\":\"7cb26e70-2842-42a1-ac74-cceeffbb15c1\",\"result\":{\"datalake.TestEvent1\":true,\"datalake.TestEvent2\":true,\"datalake.TestEvent3\":true}}")
	ds, err := parseDatasetsFromOpaBody(zap.NewNop().Sugar(), result)

	if err != nil {
		t.Fatalf("should parse : %+v", err)
	}

	if len(ds) != 3 {
		t.Fatalf("should have 3 datasets : %+v", ds)
	}
	if !slices.Contains(ds, "datalake.TestEvent1") {
		t.Fatalf("should have datalake.TestEvent1 dataset : %+v", ds)
	}
	if !slices.Contains(ds, "datalake.TestEvent2") {
		t.Fatalf("should have datalake.TestEvent2 dataset : %+v", ds)
	}
	if !slices.Contains(ds, "datalake.TestEvent3") {
		t.Fatalf("should have datalake.TestEvent3 dataset : %+v", ds)
	}
}

func Test_parse_single_dataset(t *testing.T) {
	result := []byte("{\"decision_id\":\"84b6619d-b237-44b9-ad4e-900db0c0566c\",\"result\":{\"singleDataset\":true}}")
	ds, err := parseDatasetsFromOpaBody(zap.NewNop().Sugar(), result)
	if err != nil {
		t.Fatalf("should parse : %+v", err)
	}

	if len(ds) != 1 {
		t.Fatalf("should have 1 dataset : %+v", ds)
	}
}

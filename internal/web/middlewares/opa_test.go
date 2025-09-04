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
	"testing"

	"go.uber.org/zap"
)

func Test_parse_mim_decision_admin(t *testing.T) {
	result := []byte("{\"decision_id\":\"5c78dda3-6389-4fc3-9f01-764f5ef9fe36\",\"result\":{\"*\":true}}")
	logger := zap.NewNop().Sugar()
	ds, err := parseDatasetsFromOpaBody(logger, result)

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

func Test_parse_mim_decision_datasets(t *testing.T) {
	result := []byte("{\"decision_id\":\"5c78dda3-6389-4fc3-9f01-764f5ef9fe36\",\"result\":[\"cima.AnimalBirthEvent\", \"cima.AnimalDeathEvent\"]}")
	logger := zap.NewNop().Sugar()
	ds, err := parseDatasetsFromOpaBody(logger, result)

	if err != nil {
		t.Fatalf("should parse : %+v", err)
	}
	if len(ds) != 2 {
		t.Fatalf("should have 2 datasets : %+v", ds)
	}
	if ds[0] != "cima.AnimalBirthEvent" {
		t.Fatalf("should have cima.AnimalBirthEvent dataset : %+v", ds)
	}
	if ds[1] != "cima.AnimalDeathEvent" {
		t.Fatalf("should have cima.AnimalDeathEvent dataset : %+v", ds)
	}
}

func Test_parse_datasets(t *testing.T) {
	result := []byte("{\"decision_id\":\"7cb26e70-2842-42a1-ac74-cceeffbb15c1\",\"result\":{\"datalake.NorturaLivestockCattle\":true,\"datalake.NorturaLivestockHealthEvents\":true,\"datalake.NorturaLivestockHerd\":true}}")
	logger := zap.NewNop().Sugar()
	ds, err := parseDatasetsFromOpaBody(logger, result)

	if err != nil {
		t.Fatalf("should parse : %+v", err)
	}

	if len(ds) != 3 {
		t.Fatalf("should have 3 datasets : %+v", ds)
	}
	if ds[0] != "datalake.NorturaLivestockCattle" {
		t.Fatalf("should have datalake.NorturaLivestockCattle dataset : %+v", ds)
	}
	if ds[1] != "datalake.NorturaLivestockHealthEvents" {
		t.Fatalf("should have datalake.NorturaLivestockHealthEvents dataset : %+v", ds)
	}
	if ds[2] != "datalake.NorturaLivestockHerd" {
		t.Fatalf("should have datalake.NorturaLivestockHerd dataset : %+v", ds)
	}
}

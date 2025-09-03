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

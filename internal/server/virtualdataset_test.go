package server

import (
	"errors"
	"go.uber.org/zap"
	"testing"
)

func TestVirtualDataset_StreamChanges(t *testing.T) {
	t.Run("Should return a continuation token when wrapped function is successful", func(t *testing.T) {
		dataset := &VirtualDataset{
			Wrap: func(d *VirtualDataset, params map[string]any, since string, f func(entity *Entity) error) (string, error) {
				return "token", nil
			},
		}
		token, err := dataset.StreamChanges("since", nil, func(entity *Entity) error { return nil })
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if token != "token" {
			t.Errorf("Expected token, got %v", token)
		}
	})

	t.Run("Should return error when wrapped function fails", func(t *testing.T) {
		dataset := &VirtualDataset{
			Wrap: func(d *VirtualDataset, params map[string]any, since string, f func(entity *Entity) error) (string, error) {
				return "", errors.New("error")
			},
		}
		_, err := dataset.StreamChanges("since", nil, func(entity *Entity) error { return nil })
		if err == nil {
			t.Error("Expected error, got nil")
		}

		if err.Error() != "error" {
			t.Errorf("Expected error, got %v", err)
		}
	})
}

func TestDataset_IsVirtual(t *testing.T) {
	t.Run("Should return true if the dataset is virtual", func(t *testing.T) {
		dataset := &Dataset{
			VirtualDatasetConfig: &VirtualDatasetConfig{
				Transform: "transform",
			},
		}
		if !dataset.IsVirtual() {
			t.Error("Expected true, got false")
		}
	})

	t.Run("Should return false if the dataset is virtual but misconfigured", func(t *testing.T) {
		dataset := &Dataset{
			VirtualDatasetConfig: &VirtualDatasetConfig{
				Transform: "",
			},
		}
		if dataset.IsVirtual() {
			t.Error("Expected false, got true")
		}
	})
	t.Run("Should return false if the dataset is missing virtual config", func(t *testing.T) {
		dataset := &Dataset{}
		if dataset.IsVirtual() {
			t.Error("Expected false, got true")
		}
	})
}

func TestDataset_AsVirtualDataset(t *testing.T) {
	t.Run("Should return a VirtualDataset with the same configuration", func(t *testing.T) {
		dataset := &Dataset{
			VirtualDatasetConfig: &VirtualDatasetConfig{
				Transform: "transform",
			},
			store: &Store{
				logger: zap.NewNop().Sugar(),
			},
		}
		virtualDataset := dataset.AsVirtualDataset(nil, nil)
		if virtualDataset.VirtualDatasetConfig != dataset.VirtualDatasetConfig {
			t.Error("Expected same configuration, got different")
		}
	})
}

package secrets

import "go.uber.org/zap"

type SecretStore interface {
	HasKey(key string) bool
	Value(key string) (string, bool)
	Params() *map[string]interface{}
}

type NoopStore struct{}

func (s *NoopStore) HasKey(key string) bool {
	return false
}

func (s *NoopStore) Value(key string) (string, bool) {
	return "", false
}

func (s *NoopStore) Params() *map[string]interface{} {
	params := make(map[string]interface{})
	return &params
}

func NewManager(managerType string, profile string, logger *zap.SugaredLogger) (SecretStore, error) {
	if managerType == "ssm" {
		// attempt at loading values from ssm
		logger.Info("Using AWS SSM secrets manager")
		return NewSsm(&SsmManagerConfig{
			Env:    profile,
			Key:    "/application/datahub/",
			Logger: logger,
		})
	} else {
		logger.Info("Using NOOP secrets manager")
		return &NoopStore{}, nil
	}
}

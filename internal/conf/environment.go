// Copyright 2021 MIMIRO AS
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

package conf

import (
	"time"

	"go.uber.org/zap"
)

type Env struct {
	Logger                  *zap.SugaredLogger
	Env                     string
	Port                    string
	StoreLocation           string
	BackupLocation          string
	BackupSchedule          string
	BackupRsync             bool
	AgentHost               string
	SecretsManager          string
	Auth                    *AuthConfig
	DlJwtConfig             *DatalayerJwtConfig
	GcOnStartup             bool
	FullsyncLeaseTimeout    time.Duration
	BlockCacheSize          int64
	ValueLogFileSize        int64
	AdminUserName           string
	AdminPassword           string
	NodeId                  string
	SecurityStorageLocation string
	BackupSourceLocation    string
	RunnerConfig            *RunnerConfig
}

type AuthConfig struct {
	WellKnown  string
	Audience   string
	Issuer     string
	Middleware string
}

type DatalayerJwtConfig struct {
	ClientId     string
	ClientSecret string
	Audience     string
	GrantType    string
	Endpoint     string
}

// RunnerConfig sets the initial config for the underlying job runner.
// PoolIncremental defines the max number of jobs that can be ran at once
// Concurrent defines how many of the same EntryID should be allowed, this should always be 0 in the datahub
type RunnerConfig struct {
	PoolIncremental int
	PoolFull        int
	Concurrent      int
}

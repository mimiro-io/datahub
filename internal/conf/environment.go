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
	"go.uber.org/zap"
	"time"
)

type Env struct {
	Logger               *zap.SugaredLogger
	Env                  string
	Port                 string
	StoreLocation        string
	BackupLocation       string
	BackupSchedule       string
	BackupRsync          bool
	AgentHost            string
	Auth                 *AuthConfig
	DlJwtConfig          *DatalayerJwtConfig
	GcOnStartup          bool
	FullsyncLeaseTimeout time.Duration
	BlockCacheSize       int64
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

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
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var filePtr = flag.String("config", "", "Optional path to the config file")

// NewEnv bootstraps the environment from the .env files, but also
// takes care of getting secrets from the secrets store.
// It will first start a logger to be able to log that it is loading
// an environment, and as soon as it knows more about that, it will
// switch to the correct logger
// You can give it a basePath, this is great for testing, but by default
// this is set to "."
func NewEnv() (*Env, error) {
	flag.Parse()
	return loadEnv(filePtr, true)
}

func loadEnv(basePath *string, loadFromHome bool) (*Env, error) {
	profile, found := os.LookupEnv("PROFILE")
	if !found {
		profile = "local"
	}

	logger := GetLogger(profile, zapcore.InfoLevel) // add a default logger while loading the env
	logger.Infof("Loading logging profile: %s", profile)

	err := parseEnv(basePath, logger, loadFromHome)
	if err != nil {
		return nil, err
	}

	return &Env{
		Logger:         logger,
		Env:            profile,
		Port:           viper.GetString("SERVER_PORT"),
		StoreLocation:  viper.GetString("STORE_LOCATION"),
		BackupLocation: viper.GetString("BACKUP_LOCATION"),
		BackupSchedule: viper.GetString("BACKUP_SCHEDULE"),
		BackupRsync:    viper.GetBool("BACKUP_USE_RSYNC"),
		AgentHost:      viper.GetString("DD_AGENT_HOST"),
		SecretsManager: viper.GetString("SECRETS_MANAGER"),
		Auth: &AuthConfig{
			WellKnown:  viper.GetString("TOKEN_WELL_KNOWN"),
			Audience:   viper.GetString("TOKEN_AUDIENCE"),
			Issuer:     viper.GetString("TOKEN_ISSUER"),
			Middleware: viper.GetString("AUTHORIZATION_MIDDLEWARE"),
		},
		DlJwtConfig: &DatalayerJwtConfig{
			ClientId:     viper.GetString("DL_JWT_CLIENT_ID"),
			ClientSecret: viper.GetString("DL_JWT_CLIENT_SECRET"),
			Audience:     viper.GetString("DL_JWT_AUDIENCE"),
			GrantType:    viper.GetString("DL_JWT_GRANT_TYPE"),
			Endpoint:     viper.GetString("DL_JWT_ENDPOINT"),
		},
		GcOnStartup:             viper.GetBool("GC_ON_STARTUP"),
		FullsyncLeaseTimeout:    viper.GetDuration("FULLSYNC_LEASE_TIMEOUT"),
		BlockCacheSize:          viper.GetInt64("BLOCK_CACHE_SIZE"),
		ValueLogFileSize:        viper.GetInt64("VALUE_LOG_FILE_SIZE"),
		AdminUserName:           viper.GetString("ADMIN_USERNAME"),
		AdminPassword:           viper.GetString("ADMIN_PASSWORD"),
		NodeId:                  viper.GetString("NODE_ID"),
		SecurityStorageLocation: viper.GetString("SECURITY_STORAGE_LOCATION"),
	}, nil
}

func parseEnv(basePath *string, logger *zap.SugaredLogger, loadFromHome bool) error {
	path := "."
	configFile := ".env"
	if basePath != nil && *basePath != "" {
		// if a base path is set, we expect the full path
		runes := []rune(*basePath)
		if string(runes[len(runes)-1:]) == "/" {
			path = *basePath
		} else {
			last := strings.LastIndex(*basePath, "/")
			if last > -1 {
				// some rune magic to get the path + the file name
				path = string(runes[0 : last+1])
				configFile = string(runes[last+1:])
			}
		}
	}

	// set up a default db location, if possible
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/tmp"
	}

	// we need to set all the env keys as default, so that they can be picked from the env automatically
	viper.SetDefault("SERVER_PORT", "8080")
	viper.SetDefault("LOG_LEVEL", "INFO")
	viper.SetDefault("STORE_LOCATION", fmt.Sprintf("%s/%s", home, "datahub"))
	viper.SetDefault("TOKEN_WELL_KNOWN", "")
	viper.SetDefault("TOKEN_AUDIENCE", "")
	viper.SetDefault("TOKEN_ISSUER", "")
	viper.SetDefault("DD_AGENT_HOST", "")
	viper.SetDefault("DL_JWT_CLIENT_ID", "")
	viper.SetDefault("DL_JWT_CLIENT_SECRET", "")
	viper.SetDefault("DL_JWT_AUDIENCE", "")
	viper.SetDefault("DL_JWT_GRANT_TYPE", "")
	viper.SetDefault("DL_JWT_ENDPOINT", "")
	viper.SetDefault("AUTHORIZATION_MIDDLEWARE", "noop")
	viper.SetDefault("OPA_ENDPOINT", "")
	viper.SetDefault("BACKUP_LOCATION", "")
	viper.SetDefault("BACKUP_SCHEDULE", "*/5 * * * *") // every 5 mins
	viper.SetDefault("BACKUP_USE_RSYNC", "true")
	viper.SetDefault("SECRETS_MANAGER", "noop") // turned off by default
	viper.SetDefault("GC_ON_STARTUP", "true")
	viper.SetDefault("FULLSYNC_LEASE_TIMEOUT", "1h")

	viper.SetDefault("NODE_ID", "anonymous-node")
	viper.SetDefault("SECURITY_STORAGE_LOCATION", fmt.Sprintf("%s/%s", home, "datahubsecurity"))

	viper.AutomaticEnv()

	viper.SetConfigType("env")
	viper.SetConfigName(configFile)
	viper.AddConfigPath(path)
	if loadFromHome {
		viper.AddConfigPath("$HOME/.datahub") // look for a .env file in home
	}
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			logger.Info("Config file was not found, using env variables only")
		} else {
			// Config file was found but another error was produced
			logger.DPanicf("Fatal error config file: %s", err)
			return err
		}
	} else {
		logger.Infof("Reading config file %s", viper.GetViper().ConfigFileUsed())
	}

	return nil
}

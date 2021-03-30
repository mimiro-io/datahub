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
	"os"
	"testing"

	"github.com/franela/goblin"
	"github.com/spf13/viper"
)

func TestLoadEnv(t *testing.T) {
	g := goblin.Goblin(t)

	var currentDir string

	g.Describe("Loading the Environment", func() {
		g.Before(func() {
			_ = os.Setenv("PROFILE", "test")
			viper.Reset()
			d, err := os.Getwd()
			if err != nil {
				g.Fail(err)
			}
			currentDir = d + "/../../.env-test"

		})
		g.It("should parse the test env file", func() {
			env, err := loadEnv(&currentDir, false)
			if err != nil {
				g.Fail(err)
			}
			g.Assert(env.Env).Equal("test")
			g.Assert(env.Port).Equal("9999")
		})

		g.After(func() {
			_ = os.Unsetenv("PROFILE")
		})
	})

}

func TestParseEnvFromFile(t *testing.T) {
	g := goblin.Goblin(t)

	var currentDir string
	var env *Env
	g.Describe("Parsing the env file", func() {
		g.Before(func() {
			_ = os.Setenv("PROFILE", "test")
			viper.Reset()
			d, err := os.Getwd()
			if err != nil {
				g.Fail(err)
			}
			currentDir = d + "/../../.env-test"
			e, err := loadEnv(&currentDir, false)
			if err != nil {
				g.Fail(err)
			}
			env = e
		})
		g.It("should have correct values", func() {
			g.Assert(env.Port).Equal("9999")
			g.Assert(env.StoreLocation).Equal("./test")
			g.Assert(env.AgentHost).Equal("127.0.0.1:8125")

		})

		g.It("should have correct dl jwt values", func() {
			g.Assert(env.DlJwtConfig.ClientId).Equal("id1")
			g.Assert(env.DlJwtConfig.ClientSecret).Equal("some-secret")
			g.Assert(env.DlJwtConfig.Audience).Equal("yes")
			g.Assert(env.DlJwtConfig.GrantType).Equal("test")
			g.Assert(env.DlJwtConfig.Endpoint).Equal("http://localhost")
		})

		g.It("should have correct auth values", func() {
			g.Assert(env.Auth.WellKnown).Equal("https://example.io/jwks/.well-known/jwks.json")
			g.Assert(env.Auth.Audience).Equal("https://example.io")
			g.Assert(env.Auth.Issuer).Equal("https://example.io")
			g.Assert(env.Auth.Middleware).Equal("noop")

		})

		g.It("should have opa configuration", func() {
			g.Assert(viper.GetString("OPA_ENDPOINT")).Equal("http://localhost:1111")
		})

		g.It("should have correct backup values", func() {
			g.Assert(env.BackupLocation).Equal("./backup")
			g.Assert(env.BackupSchedule).Equal("*/15 * * * *")
			g.Assert(env.BackupRsync).IsTrue()
		})

		g.After(func() {
			_ = os.Unsetenv("PROFILE")
		})
	})
}

func TestLoadEnvNoFile(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("Loading the env without a file", func() {
		g.Before(func() {
			_ = os.Setenv("PROFILE", "test")
			viper.Reset()
		})
		g.It("should load fine", func() {
			_, err := loadEnv(nil, false)
			if err != nil {
				g.Fail(err)
			}
		})

		g.After(func() {
			_ = os.Unsetenv("PROFILE")
		})
	})
}

func TestLoadEnvVariables(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("Loading env from env variables only", func() {
		var c *Env

		g.Before(func() {
			viper.Reset()
			_ = os.Setenv("PROFILE", "test")
			_ = os.Setenv("TOKEN_WELL_KNOWN", "https://example.io/jwks/.well-known/jwks.json")
			_ = os.Setenv("DL_JWT_CLIENT_ID", "12345")
			cf, err := loadEnv(nil, false)
			if err != nil {
				g.Fail(err)
			}
			c = cf
		})

		g.It("should have read some env variables", func() {
			g.Assert(c.Auth.WellKnown).Equal("https://example.io/jwks/.well-known/jwks.json")
			g.Assert(c.DlJwtConfig.ClientId).Equal("12345")
		})

		g.It("should have set some default values", func() {
			g.Assert(c.BackupSchedule).Equal("*/5 * * * *")
			g.Assert(c.BackupRsync).IsTrue()
		})

		g.After(func() {
			_ = os.Unsetenv("PROFILE")
			_ = os.Unsetenv("TOKEN_WELL_KNOWN")
			_ = os.Unsetenv("DL_JWT_CLIENT_ID")
		})
	})
}

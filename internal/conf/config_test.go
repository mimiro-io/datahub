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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"
)

func TestConf(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Conf Suite")
}

var _ = Describe("Loading the Environment", Ordered, func() {
	var currentDir string
	BeforeAll(func() {
		_ = os.Setenv("PROFILE", "test")
		viper.Reset()
		d, err := os.Getwd()
		if err != nil {
			Fail(err.Error())
		}
		currentDir = d + "/../../.env-test"
	})
	It("should parse the test env file", func() {
		env, err := loadEnv(currentDir, false)
		if err != nil {
			Fail(err.Error())
		}
		Expect(env.Env).To(Equal("test"))
		Expect(env.Port).To(Equal("9999"))
	})

	AfterAll(func() {
		_ = os.Unsetenv("PROFILE")
	})
})

var _ = Describe("Parsing the env file", Ordered, func() {
	var currentDir string
	var env *Env
	BeforeAll(func() {
		_ = os.Setenv("PROFILE", "test")
		viper.Reset()
		d, err := os.Getwd()
		if err != nil {
			Fail(err.Error())
		}
		currentDir = d + "/../../.env-test"
		e, err := loadEnv(currentDir, false)
		if err != nil {
			Fail(err.Error())
		}
		env = e
	})
	It("should have correct values", func() {
		Expect(env.Port).To(Equal("9999"))
		Expect(env.StoreLocation).To(Equal("./test"))
		Expect(env.AgentHost).To(Equal("127.0.0.1:8125"))
	})

	It("should have correct dl jwt values", func() {
		Expect(env.DlJwtConfig.ClientID).To(Equal("id1"))
		Expect(env.DlJwtConfig.ClientSecret).To(Equal("some-secret"))
		Expect(env.DlJwtConfig.Audience).To(Equal("yes"))
		Expect(env.DlJwtConfig.GrantType).To(Equal("test"))
		Expect(env.DlJwtConfig.Endpoint).To(Equal("http://localhost"))
	})

	It("should have correct auth values", func() {
		Expect(env.Auth.WellKnown).To(Equal("https://example.io/jwks/.well-known/jwks.json"))
		Expect(env.Auth.Audience).To(Equal([]string{"https://example.io"}))
		Expect(env.Auth.Issuer).To(Equal([]string{"https://example.io"}))
		Expect(env.Auth.Middleware).To(Equal("noop"))
	})

	It("should have opa configuration", func() {
		Expect(viper.GetString("OPA_ENDPOINT")).To(Equal("http://localhost:1111"))
	})

	It("should have correct backup values", func() {
		Expect(env.BackupLocation).To(Equal("./backup"))
		Expect(env.BackupSchedule).To(Equal("*/15 * * * *"))
		Expect(env.BackupRsync).To(BeTrue())
	})

	AfterAll(func() {
		_ = os.Unsetenv("PROFILE")
	})
})

var _ = Describe("Loading the env without a file", Ordered, func() {
	BeforeAll(func() {
		_ = os.Setenv("PROFILE", "test")
		viper.Reset()
	})
	It("should load fine", func() {
		_, err := loadEnv("", false)
		if err != nil {
			Fail(err.Error())
		}
	})

	AfterAll(func() {
		_ = os.Unsetenv("PROFILE")
	})
})

var _ = Describe("Loading env from env variables only", Ordered, func() {
	var c *Env

	BeforeAll(func() {
		viper.Reset()
		_ = os.Setenv("PROFILE", "test")
		_ = os.Setenv("TOKEN_WELL_KNOWN", "https://example.io/jwks/.well-known/jwks.json")
		_ = os.Setenv("DL_JWT_CLIENT_ID", "12345")
		cf, err := loadEnv("", false)
		if err != nil {
			Fail(err.Error())
		}
		c = cf
	})

	It("should have read some env variables", func() {
		Expect(c.Auth.WellKnown).To(Equal("https://example.io/jwks/.well-known/jwks.json"))
		Expect(c.DlJwtConfig.ClientID).To(Equal("12345"))
	})

	It("should have set some default values", func() {
		Expect(c.BackupSchedule).To(Equal("*/5 * * * *"))
		Expect(c.BackupRsync).To(BeTrue())
	})

	AfterAll(func() {
		_ = os.Unsetenv("PROFILE")
		_ = os.Unsetenv("TOKEN_WELL_KNOWN")
		_ = os.Unsetenv("DL_JWT_CLIENT_ID")
	})
})

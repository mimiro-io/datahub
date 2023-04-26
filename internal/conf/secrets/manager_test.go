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

package secrets

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

func TestNewManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "conf/Secrets Suite")
}

var _ = Describe("Testing Secrets Creation", func() {
	It("should return noop manager when given noop", func() {
		env := &conf.Env{
			Env:            "test",
			SecretsManager: "noop",
		}

		mngr, err := NewManager(env, zap.NewNop().Sugar())
		if err != nil {
			Fail(err.Error())
		}
		switch mngr.(type) {
		case *NoopStore:
			// ok
		default:
			Fail("store is not NoopStore")
		}
	})
})

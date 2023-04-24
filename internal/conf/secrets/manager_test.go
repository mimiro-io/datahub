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
	"errors"
	"testing"

	"github.com/franela/goblin"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

func TestNewManager(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("Testing Secrets Creation", func() {
		g.It("should return noop manager when given noop", func() {
			env := &conf.Env{
				Env:            "test",
				SecretsManager: "noop",
			}

			mngr, err := NewManager(env, zap.NewNop().Sugar())
			if err != nil {
				g.Fail(err)
			}
			switch mngr.(type) {
			case *NoopStore:
				// ok
			default:
				g.Fail(errors.New("store is not NoopStore"))
			}
		})
	})
}

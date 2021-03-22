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
	"reflect"
	"testing"

	"github.com/franela/goblin"
	"go.uber.org/fx"

	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

func TestStatsdClient(t *testing.T) {
	g := goblin.Goblin(t)

	var lc fx.Lifecycle
	g.Describe("Start an instance", func() {
		g.Before(func() {
			lc = fxtest.NewLifecycle(t)
		})
		g.It("should be of noop type when no agent host", func() {
			env := &Env{
				AgentHost: "",
			}
			client, err := NewStatsD(lc, env, zap.NewNop().Sugar())
			g.Assert(err).IsNil()
			g.Assert(reflect.ValueOf(client).Type().String()).Eql("*statsd.NoOpClient")
		})

		g.It("should be statsd client when agent host set", func() {
			env := &Env{
				AgentHost: "127.0.0.1:8125",
			}
			client, err := NewStatsD(lc, env, zap.NewNop().Sugar())
			g.Assert(err).IsNil()
			g.Assert(reflect.ValueOf(client).Type().String()).Eql("*statsd.Client")
		})
	})
}

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
	"testing"

	"github.com/franela/goblin"

	"go.uber.org/zap/zapcore"
)

func TestLogger(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The Logger's Loglevel parser", func() {
		g.It("Should accept 'debug'", func() {
			g.Assert(getLogLevel("debug")).Eql(zapcore.DebugLevel)
		})
		g.It("Should accept 'info'", func() {
			g.Assert(getLogLevel("info")).Eql(zapcore.InfoLevel)
		})
		g.It("Should accept 'warn'", func() {
			g.Assert(getLogLevel("warn")).Eql(zapcore.WarnLevel)
		})
		g.It("Should accept 'ERRor'", func() {
			g.Assert(getLogLevel("ERRor")).Eql(zapcore.ErrorLevel)
		})
		g.It("Should accept fallback to info", func() {
			g.Assert(getLogLevel("LLSDFFSD")).Eql(zapcore.InfoLevel)
		})
	})
	g.Describe("Getting a logger", func() {
		g.It("Should get the correct logger", func() {
			logger := GetLogger("local", zapcore.InfoLevel).Desugar()
			g.Assert(logger.Core().Enabled(zapcore.InfoLevel)).IsTrue("Go the wrong logger")
		})
	})
}

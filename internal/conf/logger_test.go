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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
)

var _ = Describe("The Logger's Loglevel parser", func() {
	It("Should accept 'debug'", func() {
		Expect(getLogLevel("debug")).To(Equal(zapcore.DebugLevel))
	})
	It("Should accept 'info'", func() {
		Expect(getLogLevel("info")).To(Equal(zapcore.InfoLevel))
	})
	It("Should accept 'warn'", func() {
		Expect(getLogLevel("warn")).To(Equal(zapcore.WarnLevel))
	})
	It("Should accept 'ERRor'", func() {
		Expect(getLogLevel("ERRor")).To(Equal(zapcore.ErrorLevel))
	})
	It("Should accept fallback to info", func() {
		Expect(getLogLevel("LLSDFFSD")).To(Equal(zapcore.InfoLevel))
	})
})

var _ = Describe("Getting a logger", func() {
	It("Should get the correct logger", func() {
		logger := GetLogger("local", zapcore.InfoLevel).Desugar()
		Expect(logger.Core().Enabled(zapcore.InfoLevel)).To(BeTrue(), "Go the wrong logger")
	})
})

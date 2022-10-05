// Copyright 2022 MIMIRO AS
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

package internal

import "testing"

type FxLogger struct {
	t      testing.TB
	active bool
}

func (l *FxLogger) Errorf(s string, i ...interface{}) {
	l.t.Errorf(s, i...)
}

func (l *FxLogger) FailNow() {
	l.t.FailNow()
}

func (l *FxLogger) Logf(f string, a ...interface{}) {
	if l.active {
		l.t.Logf(f, a...)
	}
}
func FxTestLog(t testing.TB, active bool) *FxLogger {
	return &FxLogger{t: t, active: active}
}

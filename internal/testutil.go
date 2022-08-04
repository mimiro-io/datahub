package internal

import "testing"

type FxLogger struct {
	t      *testing.T
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
func FxTestLog(t *testing.T, active bool) *FxLogger {
	return &FxLogger{t: t, active: active}
}

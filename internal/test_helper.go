package internal

import "testing"

type SwitchableLogger struct {
	T testing.TB
	Active bool
}

func (sl *SwitchableLogger) Errorf(s string, i ...interface{}) {
	sl.Logf(s, i...)
}

func (sl *SwitchableLogger) FailNow() {
	sl.T.FailNow()
}

func (sl *SwitchableLogger) Logf(tpl string, params ...interface{}) {
	if sl.Active {
		sl.T.Logf(tpl, params...)
	}
}


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

package server

import (
	"context"
	"os"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/franela/goblin"
)

func TestBackup(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The BackupManager", func() {
		var s *Store
		g.It("Should run without error", func() {

			storeLocation := "./test_store_backup"
			backupLocation := "/tmp/badger/test_store_backup_backup"

			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil()

			defer func() { // clean after the test
				_ = os.RemoveAll(storeLocation)
				_ = os.RemoveAll(backupLocation)
			}()

			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}

			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			os.Stderr = devNull
			lc := fxtest.NewLifecycle(t)
			s = NewStore(lc, e, &statsd.NoOpClient{})

			err = lc.Start(context.Background())
			g.Assert(err).IsNil()
			os.Stderr = oldErr

			backup := &BackupManager{}
			backup.store = s
			backup.backupLocation = backupLocation

			backup.lastID, err = backup.LoadLastId()
			g.Assert(err).IsNil()
			err = backup.DoNativeBackup()
			g.Assert(err).IsNil()
		})
	})
}

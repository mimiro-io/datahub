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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

func TestBackup(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The BackupManager", func() {
		var s *Store
		testCnt := 0
		var storeLocation string
		var backupLocation string
		var backup *BackupManager
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./test_store_backup_%v", testCnt)
			backupLocation = fmt.Sprintf("./test_store_backup_backup_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			err = os.RemoveAll(backupLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)

			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
			}

			lc := fxtest.NewLifecycle(internal.FxTestLog(t, false))
			s = NewStore(lc, e, &statsd.NoOpClient{})

			g.Assert(s.Open()).IsNil()

			backup = &BackupManager{}
			backup.logger = zap.NewNop().Sugar()
			backup.store = s
			backup.backupLocation = backupLocation
			backup.backupSourceLocation = storeLocation
		})
		g.AfterEach(func() {
			_ = os.RemoveAll(storeLocation)
			_ = os.RemoveAll(backupLocation)
		})

		g.It("Should perform native backup", func() {
			var err error
			backup.lastID, err = backup.LoadLastID()
			g.Assert(err).IsNil()
			backup.Run()
			// check backup id file is synced
			storageIDFile := filepath.Join(backupLocation, StorageIDFileName)
			if _, err := os.Stat(storageIDFile); errors.Is(err, os.ErrNotExist) {
				g.Errorf("expected backup id file to be copied")
				g.FailNow()
			}

			// check there is an actual backup
			if _, err := os.Stat(filepath.Join(backupLocation, "datahub-backup.kv")); errors.Is(err, os.ErrNotExist) {
				g.Errorf("expected backup file to be written")
				g.FailNow()
			}

			// restart and backup again
			s.Close()
			s.Open()
			backup.Run()
			if _, err := os.Stat(storageIDFile); errors.Is(err, os.ErrNotExist) {
				g.Errorf("expected backup id file to be copied")
				g.FailNow()
			}
		})
		g.It("Should perform rsync backup", func() {
			g.Timeout(2 * time.Minute)
			backup.useRsync = true
			var err error
			backup.lastID, err = backup.LoadLastID()
			g.Assert(err).IsNil()
			backup.Run()
			// check backup id file is synced
			storageIDFile := filepath.Join(backupLocation, StorageIDFileName)
			if _, err := os.Stat(storageIDFile); errors.Is(err, os.ErrNotExist) {
				g.Errorf("expected backup id file to be copied")
				g.FailNow()
			}
		})
		g.It("Should stop datahub if backup to invalid location", func() {
			g.Timeout(2 * time.Minute)
			backup.useRsync = true
			backup.Run()
			// check backup id file is synced
			storageIDFile := filepath.Join(backupLocation, StorageIDFileName)
			if _, err := os.Stat(storageIDFile); errors.Is(err, os.ErrNotExist) {
				g.Errorf("expected backup id file to be copied")
				g.FailNow()
			}
			// stop store, remove id file and start again - a new id file should be generated
			s.Close()
			os.Remove(filepath.Join(storeLocation, StorageIDFileName))
			s.Open()

			// backup should fail now
			assertPanic(g, func() { backup.Run() })
		})
	})
}

func assertPanic(g *goblin.G, f func()) {
	defer func() {
		if r := recover(); r == nil {
			g.Errorf("The code did not panic")
			g.FailNow()
		}
	}()
	f()
}

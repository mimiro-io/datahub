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
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

func TestServer(t *testing.T) {
	RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Server Suite")
}

var _ = ginkgo.Describe("The BackupManager", func() {
	var s *Store
	testCnt := 0
	var storeLocation string
	var backupLocation string
	var backup *BackupManager
	ginkgo.BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_store_backup_%v", testCnt)
		backupLocation = fmt.Sprintf("./test_store_backup_backup_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		err = os.RemoveAll(backupLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)

		e := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}

		// lc := fxtest.NewLifecycle(internal.FxTestLog(ginkgo.GinkgoT(), false))
		s = NewStore(e, &statsd.NoOpClient{})

		// Expect(s.Open()).To(BeNil())

		backup = &BackupManager{}
		backup.logger = zap.NewNop().Sugar()
		backup.store = s
		backup.backupLocation = backupLocation
		backup.backupSourceLocation = storeLocation
	})
	ginkgo.AfterEach(func() {
		_ = os.RemoveAll(storeLocation)
		_ = os.RemoveAll(backupLocation)
	})

	ginkgo.It("Should perform native backup", func() {
		var err error
		backup.lastID, err = backup.LoadLastID()
		Expect(err).To(BeNil())
		backup.Run()
		// check backup id file is synced
		storageIDFile := filepath.Join(backupLocation, StorageIDFileName)
		if _, err := os.Stat(storageIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected backup id file to be copied")
		}

		// check there is an actual backup
		if _, err := os.Stat(filepath.Join(backupLocation, "datahub-backup.kv")); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected backup file to be written")
		}

		// restart and backup again
		s.Close()
		s.Open()
		backup.Run()
		if _, err := os.Stat(storageIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected backup id file to be copied")
		}
	})
	ginkgo.It("Should perform rsync backup", func(_ ginkgo.SpecContext) {
		backup.useRsync = true
		var err error
		backup.lastID, err = backup.LoadLastID()
		Expect(err).To(BeNil())
		backup.Run()
		mirroredIDFile := filepath.Join(backupLocation, filepath.Base(storeLocation), StorageIDFileName)
		if _, err := os.Stat(mirroredIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected store id file to be mirrored into the backup")
		}
	}, ginkgo.SpecTimeout(2*time.Minute))
	ginkgo.It("Should compare ids at the backup root when the source is the store with a trailing slash", func(_ ginkgo.SpecContext) {
		backup.useRsync = true
		backup.backupSourceLocation = storeLocation + "/"
		backup.Run()
		storageIDFile := filepath.Join(backupLocation, StorageIDFileName)
		if _, err := os.Stat(storageIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected store id file to be mirrored to the backup root")
		}
		// second run takes the id comparison path
		backup.Run()
	}, ginkgo.SpecTimeout(2*time.Minute))
	ginkgo.It("Should stop datahub if backup to invalid location", func(_ ginkgo.SpecContext) {
		backup.useRsync = true
		backup.Run()
		mirroredIDFile := filepath.Join(backupLocation, filepath.Base(storeLocation), StorageIDFileName)
		if _, err := os.Stat(mirroredIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected store id file to be mirrored into the backup")
		}
		// stop store, remove id file and start again - a new id file should be generated
		s.Close()
		os.Remove(filepath.Join(storeLocation, StorageIDFileName))
		s.Open()

		// backup should fail now
		assertPanic(func() { backup.Run() })
	}, ginkgo.SpecTimeout(2*time.Minute))
	ginkgo.It("Should stop datahub if the backup location has content but no id file", func(_ ginkgo.SpecContext) {
		backup.useRsync = true
		err := os.MkdirAll(backupLocation, 0o700)
		Expect(err).To(BeNil())
		err = os.WriteFile(filepath.Join(backupLocation, "orphan.kv"), []byte("data"), 0o644)
		Expect(err).To(BeNil())

		assertPanic(func() { backup.Run() })
	}, ginkgo.SpecTimeout(2*time.Minute))
	ginkgo.It("Should stop datahub when the backup source does not contain the store", func(_ ginkgo.SpecContext) {
		backup.useRsync = true
		backup.backupSourceLocation = "./test_store_backup_elsewhere"

		assertPanic(func() { backup.Run() })
	}, ginkgo.SpecTimeout(2*time.Minute))
	ginkgo.It("Should fail construction when the backup source does not contain the store", func(_ ginkgo.SpecContext) {
		e := &conf.Config{
			Logger:               zap.NewNop().Sugar(),
			StoreLocation:        storeLocation,
			BackupLocation:       backupLocation,
			BackupSchedule:       "*/5 * * * *",
			BackupSourceLocation: "./test_store_backup_elsewhere",
			BackupRsync:          true,
		}

		_, err := NewBackupManager(s, e)
		Expect(err).To(HaveOccurred())
	}, ginkgo.SpecTimeout(2*time.Minute))
	ginkgo.It("Should treat a backup location holding only lost+found as empty", func(_ ginkgo.SpecContext) {
		backup.useRsync = true
		err := os.MkdirAll(filepath.Join(backupLocation, "lost+found"), 0o700)
		Expect(err).To(BeNil())

		backup.Run()

		mirroredIDFile := filepath.Join(backupLocation, filepath.Base(storeLocation), StorageIDFileName)
		if _, err := os.Stat(mirroredIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected store id file to be mirrored into the backup")
		}
	}, ginkgo.SpecTimeout(2*time.Minute))
})

var _ = ginkgo.Describe("The BackupManager with a backup source above the store location", func() {
	testCnt := 0
	var sourceLocation string
	var storeLocation string
	var backupLocation string
	var s *Store
	var backup *BackupManager
	ginkgo.BeforeEach(func() {
		testCnt += 1
		sourceLocation = fmt.Sprintf("./test_backup_source_%v", testCnt)
		storeLocation = filepath.Join(sourceLocation, "datahub.store")
		backupLocation = fmt.Sprintf("./test_backup_source_backup_%v", testCnt)
		err := os.RemoveAll(sourceLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+sourceLocation)
		err = os.RemoveAll(backupLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+backupLocation)

		e := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		s = NewStore(e, &statsd.NoOpClient{})

		backup = &BackupManager{}
		backup.logger = zap.NewNop().Sugar()
		backup.store = s
		backup.backupLocation = backupLocation
		// the trailing slash makes rsync mirror the source's contents onto the backup location root
		backup.backupSourceLocation = sourceLocation + "/"
		backup.useRsync = true
	})
	ginkgo.AfterEach(func() {
		_ = os.RemoveAll(sourceLocation)
		_ = os.RemoveAll(backupLocation)
	})

	ginkgo.It("Should compare ids inside the mirrored store across runs", func(_ ginkgo.SpecContext) {
		backup.Run()
		// the id file travels inside the mirrored store directory, so a restored store carries it
		mirroredIDFile := filepath.Join(backupLocation, "datahub.store", StorageIDFileName)
		if _, err := os.Stat(mirroredIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected store id file to be mirrored with the store")
		}
		// no id copy at the backup root, so rsync --delete has nothing to remove
		if _, err := os.Stat(filepath.Join(backupLocation, StorageIDFileName)); err == nil {
			ginkgo.Fail("expected no id file at the backup root")
		}

		// second run takes the id comparison path
		backup.Run()
		if _, err := os.Stat(mirroredIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected store id file to survive the rsync run")
		}
	}, ginkgo.SpecTimeout(2*time.Minute))

	ginkgo.It("Should place the id file before the first backup writes data", func(_ ginkgo.SpecContext) {
		Expect(backup.validLocation()).To(BeTrue())

		// a first backup torn mid-transfer must already carry its identity
		mirroredIDFile := filepath.Join(backupLocation, "datahub.store", StorageIDFileName)
		if _, err := os.Stat(mirroredIDFile); errors.Is(err, os.ErrNotExist) {
			ginkgo.Fail("expected the id file to be placed before any data")
		}
	}, ginkgo.SpecTimeout(2*time.Minute))

	ginkgo.It("Should stop datahub when an empty store points at an existing backup", func(_ ginkgo.SpecContext) {
		backup.Run()
		mirroredIDFile := filepath.Join(backupLocation, "datahub.store", StorageIDFileName)
		backedUpID, err := os.ReadFile(mirroredIDFile)
		Expect(err).To(BeNil())

		// wipe the store and start over, as after losing the store volume
		s.Close()
		err = os.RemoveAll(sourceLocation)
		Expect(err).To(BeNil())
		s.Open()

		assertPanic(func() { backup.Run() })

		// the previous store's backup must be untouched
		current, err := os.ReadFile(mirroredIDFile)
		Expect(err).To(BeNil())
		Expect(current).To(Equal(backedUpID))
	}, ginkgo.SpecTimeout(2*time.Minute))
})

func assertPanic(f func()) {
	defer func() {
		if r := recover(); r == nil {
			ginkgo.Fail("The code did not panic")
		}
	}()
	f()
}

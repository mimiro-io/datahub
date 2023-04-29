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
	"encoding/binary"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/bamzi/jobrunner"
	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
)

const StorageIDFileName = "DATAHUB_BACKUPID"

type BackupManager struct {
	backupLocation       string
	backupSourceLocation string
	schedule             string
	useRsync             bool
	lastID               uint64
	isRunning            bool
	store                *Store
	logger               *zap.SugaredLogger
}

func NewBackupManager(lc fx.Lifecycle, store *Store, env *conf.Env) (*BackupManager, error) {
	if env.BackupLocation == "" {
		return nil, nil
	}

	if env.BackupSchedule == "" {
		return nil, nil
	}

	backup := &BackupManager{}
	backup.backupLocation = env.BackupLocation
	backup.schedule = env.BackupSchedule
	if env.BackupSourceLocation == "" {
		backup.backupSourceLocation = env.StoreLocation
	} else {
		backup.backupSourceLocation = env.BackupSourceLocation
	}
	backup.useRsync = env.BackupRsync
	backup.store = store
	backup.logger = env.Logger.Named("backup")

	// load last id
	lastID, err := backup.LoadLastID()
	if err != nil {
		return nil, err
	}
	backup.lastID = lastID

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			backup.logger.Info("init backup via hook")
			schedule, err := cron.ParseStandard(backup.schedule)
			if err != nil {
				backup.logger.Error("bad cron schedule for backup job")
				return err
			}

			// pass in this backupManager struct. function Run will be called by the scheduler
			jobrunner.MainCron.Schedule(schedule, jobrunner.New(backup))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return nil
		},
	})

	return backup, nil
}

// This is the function called by the cron job scheduler
func (backupManager *BackupManager) Run() {
	if backupManager.isRunning {
		return
	}

	backupManager.isRunning = true

	if backupManager.validLocation() {
		if backupManager.useRsync {
			err := backupManager.DoRsyncBackup()
			if err != nil {
				backupManager.logger.Error("Error with rsync backup: " + err.Error())
			}
		} else {
			err := backupManager.DoNativeBackup()
			if err != nil {
				backupManager.logger.Error("Error with native backup: " + err.Error())
				panic("Error with native backup")
			}
		}
	} else {
		backupManager.logger.Panicf("invalid backup location. %v out of sync", StorageIDFileName)
	}

	backupManager.isRunning = false
}

func (backupManager *BackupManager) DoRsyncBackup() error {
	err := os.MkdirAll(backupManager.backupLocation, 0o700)
	if err != nil {
		return err
	}

	cmd := exec.Command("rsync", "-avz", "--delete", backupManager.backupSourceLocation, backupManager.backupLocation)
	err = cmd.Run()
	return err
}

func (backupManager *BackupManager) DoNativeBackup() error {
	err := os.MkdirAll(backupManager.backupLocation, 0o700)
	if err != nil {
		return err
	}
	backupFilename := backupManager.backupLocation + string(os.PathSeparator) + "datahub-backup.kv"
	var file *os.File
	if backupManager.fileExists(backupFilename) {
		file, _ = os.Open(backupFilename)
	} else {
		file, _ = os.Create(backupFilename)
	}
	defer file.Close()
	since, _ := backupManager.store.database.Backup(file, backupManager.lastID)
	backupManager.lastID = since

	// store last id
	return backupManager.StoreLastID()
}

func (backupManager *BackupManager) StoreLastID() error {
	lastIDFilename := backupManager.backupLocation + string(os.PathSeparator) + "datahub-backup.lastseen"
	file, _ := os.Create(lastIDFilename)
	defer file.Close()

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, backupManager.lastID)
	_, err := file.Write(data)
	return err
}

func (backupManager *BackupManager) LoadLastID() (uint64, error) {
	lastIDFilename := backupManager.backupLocation + string(os.PathSeparator) + "datahub-backupManager.lastseen"
	file, err := os.Open(lastIDFilename)
	if err != nil {
		return 0, nil
	}
	defer file.Close()

	data := make([]byte, 8)
	_, err = file.Read(data)
	if err != nil {
		return 0, err
	}
	lastID := binary.LittleEndian.Uint64(data)
	return lastID, nil
}

func (backupManager *BackupManager) fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func (backupManager *BackupManager) validLocation() bool {
	dhIDFile := filepath.Join(backupManager.store.storeLocation, StorageIDFileName)
	b, err := os.ReadFile(dhIDFile)
	if err != nil {
		backupManager.logger.Warnf("could not read DATAHUB_BACKUPID file")
		return false
	}
	dhID := string(b)
	backedUpDhIDFile := filepath.Join(backupManager.backupLocation, StorageIDFileName)
	b, err = os.ReadFile(backedUpDhIDFile)
	if err != nil {
		backupManager.logger.Infof("could not read DATAHUB_BACKUPID file at backup location. copying now")
		source, err := os.Open(dhIDFile)
		if err != nil {
			backupManager.logger.Errorf("Could not open backup id file at %v.", dhIDFile)
			return false
		}
		defer source.Close()

		err = os.MkdirAll(backupManager.backupLocation, 0o700)
		if err != nil {
			backupManager.logger.Errorf("Could create backup dir at %v. (%w)", backupManager.backupLocation, err)
			return false
		}
		destination, err := os.Create(backedUpDhIDFile)
		if err != nil {
			backupManager.logger.Errorf("Could not open backed up id file at %v. (%w)", backedUpDhIDFile, err)
			return false
		}
		defer destination.Close()
		_, err = io.Copy(destination, source)
		if err != nil {
			backupManager.logger.Errorf("Could not copý %v to %v.", dhIDFile, backedUpDhIDFile)
			return false
		}
		return true
	}
	buDhID := string(b)
	return dhID == buDhID
}

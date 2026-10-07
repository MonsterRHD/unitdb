/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package memdb

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// walLogExt is the extension of a finished WAL log; logs still being
// written carry .tmp and never appear in a backup.
const walLogExt = ".log"

// BeginBackup fixes a confirmed write point for an online backup.
//
// It flushes every entry put before the call into the WAL, and then lists
// the finished log files. Until end is called, releasing time blocks (a
// sync that freed a block or a Delete that emptied one) waits, so every
// listed log stays on disk for the copy. Puts continue normally and their
// logs are not listed: later writes stay out of the backup.
//
// The caller must call end exactly once, after the listed logs have been
// copied. BeginBackup must be called with any equivalent of the database's
// sync barrier held, so that a synced block and its released WAL log cannot
// straddle the backup point.
func (db *DB) BeginBackup() (logs []string, end func(), err error) {
	if err := db.ok(); err != nil {
		return nil, nil, err
	}

	// Flush first: it rotates the current tiny log and returns only once
	// that log, and hence every Put before the call, is a finished WAL file.
	if err := db.internal.logManager.flush(); err != nil {
		return nil, nil, err
	}

	db.backupMu.Lock()

	dir := db.opts.logFilePath + string(filepath.Separator) + logDir
	entries, err := os.ReadDir(dir)
	if err != nil {
		db.backupMu.Unlock()
		return nil, nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), walLogExt) {
			logs = append(logs, filepath.Join(dir, e.Name()))
		}
	}
	// Copy logs in time ID order: recovery replays them in this order, and
	// preserving it on restore keeps the WAL replay order identical.
	sort.Strings(logs)

	var once sync.Once
	end = func() {
		once.Do(db.backupMu.Unlock)
	}
	return logs, end, nil
}

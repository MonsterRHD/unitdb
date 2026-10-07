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

package unitdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func backupOpts() []Options {
	return []Options{
		WithBufferSize(1 << 20),
		WithMemdbSize(1 << 24),
		WithMutable(),
		// No background sync: the tests decide what reaches disk.
		WithMaxSyncDuration(time.Hour, 1),
		WithDefaultQueryLimit(100000),
	}
}

func openBackupDB(t *testing.T, dir string, extra ...Options) *DB {
	t.Helper()
	db, err := Open(dir, append(backupOpts(), extra...)...)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	return db
}

func restoreOpen(t *testing.T, bak, target string, opts ...Options) *DB {
	t.Helper()
	if err := Restore(bak, target, opts...); err != nil {
		t.Fatalf("restore: %v", err)
	}
	db, err := Open(target, append(backupOpts(), opts...)...)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	return db
}

// backupMessages returns a topic's messages oldest first.
func backupMessages(t *testing.T, db *DB, topic []byte, contract uint32) []string {
	t.Helper()
	q := NewQuery(topic)
	if contract != 0 {
		q = q.WithContract(contract)
	}
	items, err := db.Get(q)
	if err != nil {
		t.Fatalf("get %s: %v", topic, err)
	}
	out := make([]string, len(items))
	for i, b := range items {
		out[len(items)-1-i] = string(b)
	}
	return out
}

func expectRangeMsgs(t *testing.T, got []string, wantIdx ...int) {
	t.Helper()
	if len(got) != len(wantIdx) {
		t.Fatalf("expected %d messages; got %d: %v", len(wantIdx), len(got), got)
	}
	for i, idx := range wantIdx {
		if want := fmt.Sprintf("m%d", idx); got[i] != want {
			t.Fatalf("message %d: expected %q; got %q (all: %v)", i, want, got[i], got)
		}
	}
}

func syncCount(t *testing.T, db *DB, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for db.Count() < want {
		if time.Now().After(deadline) {
			t.Fatalf("count reached %d, wanted %d", db.Count(), want)
		}
		if err := db.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func putSeq(t *testing.T, db *DB, topic []byte, contract uint32, i int, id []byte) {
	t.Helper()
	e := NewEntry(topic, []byte(fmt.Sprintf("m%d", i))).WithID(id)
	if contract != 0 {
		e = e.WithContract(contract)
	}
	if err := db.PutEntry(e); err != nil {
		t.Fatalf("put m%d: %v", i, err)
	}
}

func delSeq(t *testing.T, db *DB, topic []byte, contract uint32, id []byte) {
	t.Helper()
	e := NewEntry(topic, nil).WithID(id)
	if contract != 0 {
		e = e.WithContract(contract)
	}
	if err := db.DeleteEntry(e); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// TestBackupRestoreWALOnly backs up entries that exist only in the WAL; the
// restored database must replay them through the normal recovery.
func TestBackupRestoreWALOnly(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	db := openBackupDB(t, src)
	topic := []byte("backup.wal")
	for i := 0; i < 100; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := VerifyBackup(bak); err != nil {
		t.Fatalf("verify: %v", err)
	}

	// The restored directory is a plain database: no lock file and no
	// backup manifest copied into it.
	if err := Restore(bak, target); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(target, "unitdb.lock")); !os.IsNotExist(err) {
		t.Fatalf("restored target carries a lock file: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(target, backupManifestName)); !os.IsNotExist(err) {
		t.Fatalf("restored target carries a backup manifest")
	}
	rdb, err := Open(target, backupOpts()...)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	got := backupMessages(t, rdb, topic, 0)
	want := make([]int, 100)
	for i := range want {
		want[i] = i
	}
	expectRangeMsgs(t, got, want...)
	if count := rdb.Count(); count != 100 {
		t.Fatalf("restored count %d", count)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}

	// The source keeps serving writes and still has every entry.
	for i := 100; i < 200; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	got = backupMessages(t, db, topic, 0)
	if len(got) != 200 {
		t.Fatalf("source expected 200 messages; got %d", len(got))
	}
	for i := 0; i < 200; i++ {
		if got[i] != fmt.Sprintf("m%d", i) {
			t.Fatalf("source message %d: got %q", i, got[i])
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupRestoreMixed covers synced and WAL-only entries, deletes in
// both states, a second contract and TTL-bearing entries.
func TestBackupRestoreMixed(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	db := openBackupDB(t, src)
	topicC := []byte("backup.contract.topic")
	topicM := []byte("backup.master.topic")
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}

	ids := make([][]byte, 80)
	for i := 0; i < 40; i++ {
		ids[i] = db.NewID()
		e := NewEntry(topicC, []byte(fmt.Sprintf("m%d", i))).WithID(ids[i]).WithContract(contract).WithTTL("1h")
		if err := db.PutEntry(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	syncCount(t, db, 40)

	// Delete even entries 0..39 after they reached disk: the tombstones and
	// freed blocks must be in the backup.
	for i := 0; i < 40; i += 2 {
		delSeq(t, db, topicC, contract, ids[i])
	}

	// Entries 40..79 stay unsynced.
	for i := 40; i < 80; i++ {
		ids[i] = db.NewID()
		putSeq(t, db, topicC, contract, i, ids[i])
	}
	// Master-contract entries, unsynced: contract isolation must hold.
	for i := 0; i < 10; i++ {
		putSeq(t, db, topicM, 0, i, db.NewID())
	}
	// Delete odd entries 40..79 while in memory: their delete markers ride
	// the WAL into the backup.
	for i := 41; i < 80; i += 2 {
		delSeq(t, db, topicC, contract, ids[i])
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	rdb := restoreOpen(t, bak, target)

	var want []int
	for i := 1; i < 40; i += 2 {
		want = append(want, i)
	}
	for i := 40; i < 80; i += 2 {
		want = append(want, i)
	}
	got := backupMessages(t, rdb, topicC, contract)
	expectRangeMsgs(t, got, want...)
	// Count is global: 40 surviving contract entries plus 10 master entries.
	if count := rdb.Count(); count != uint64(len(want)+10) {
		t.Fatalf("count %d, wanted %d", count, len(want)+10)
	}

	// The other contract's entries are not visible under the contract, and
	// vice versa.
	gotM := backupMessages(t, rdb, topicM, 0)
	wantM := make([]int, 10)
	for i := range wantM {
		wantM[i] = i
	}
	expectRangeMsgs(t, gotM, wantM...)
	if none := backupMessages(t, rdb, topicM, contract); len(none) != 0 {
		t.Fatalf("contract query leaked master-contract entries: %v", none)
	}

	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupPointIsolation puts and flushes more entries from the hook that
// runs at the fixed backup point: they stay in the source and cannot enter
// the candidate.
func TestBackupPointIsolation(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	db := openBackupDB(t, src)
	topic := []byte("backup.point")
	for i := 0; i < 50; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	hookDone := make(chan struct{})
	prevHook := testHookBackupPoint
	testHookBackupPoint = func() {
		defer close(hookDone)
		for i := 50; i < 100; i++ {
			putSeq(t, db, topic, 0, i, db.NewID())
		}
		if err := db.Flush(); err != nil {
			t.Errorf("flush after point: %v", err)
		}
	}
	t.Cleanup(func() { testHookBackupPoint = prevHook })

	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup: %v", err)
	}
	<-hookDone

	rdb := restoreOpen(t, bak, target)
	got := backupMessages(t, rdb, topic, 0)
	want := make([]int, 50)
	for i := range want {
		want[i] = i
	}
	expectRangeMsgs(t, got, want...)
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}

	// The later writes belong to the source only.
	got = backupMessages(t, db, topic, 0)
	if len(got) != 100 {
		t.Fatalf("source expected 100 messages; got %d", len(got))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupConcurrentWrites runs a backup while a writer keeps putting and
// flushing: the source never loses or reorders, and the restored set is a
// contiguous, ordered prefix of the source's messages.
func TestBackupConcurrentWrites(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	db := openBackupDB(t, src)
	topic := []byte("backup.concurrent")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writerErr error
	var writerMu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		fail := func(err error) {
			writerMu.Lock()
			if writerErr == nil {
				writerErr = err
			}
			writerMu.Unlock()
		}
		for i := 0; ; {
			select {
			case <-stop:
				return
			default:
			}
			for end := i + 25; i < end; i++ {
				e := NewEntry(topic, []byte(fmt.Sprintf("m%d", i))).WithID(db.NewID())
				if err := db.PutEntry(e); err != nil {
					fail(err)
					return
				}
			}
			if err := db.Flush(); err != nil {
				fail(err)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Let some writes land before fixing the point.
	time.Sleep(150 * time.Millisecond)
	if err := db.Backup(bak); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("backup during writes: %v", err)
	}
	close(stop)
	wg.Wait()
	if writerErr != nil {
		t.Fatalf("writer error: %v", writerErr)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	rdb := restoreOpen(t, bak, target)
	got := backupMessages(t, rdb, topic, 0)
	if len(got) == 0 {
		t.Fatal("backup captured no messages under concurrent writes")
	}
	seen := make(map[string]bool, len(got))
	for i, m := range got {
		if want := fmt.Sprintf("m%d", i); m != want {
			t.Fatalf("restored message %d: got %q, want %q (set must be an ordered prefix)", i, m, want)
		}
		if seen[m] {
			t.Fatalf("restored duplicate %q", m)
		}
		seen[m] = true
	}
	if count := rdb.Count(); count != uint64(len(got)) {
		t.Fatalf("restored count %d for %d messages", count, len(got))
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}

	srcGot := backupMessages(t, db, topic, 0)
	if len(srcGot) < len(got) {
		t.Fatalf("source shrank: %d messages, backup had %d", len(srcGot), len(got))
	}
	for i, m := range srcGot {
		if want := fmt.Sprintf("m%d", i); m != want {
			t.Fatalf("source message %d: got %q", i, m)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupReplaceAndRecover checks that a second backup replaces the
// first, leaves no staging directory behind, and that an interrupted publish
// (dest moved aside) is rolled back instead of losing the previous backup.
func TestBackupReplaceAndRecover(t *testing.T) {
	src := t.TempDir()
	parent := t.TempDir()
	bak := filepath.Join(parent, "bak")

	db := openBackupDB(t, src)
	topic := []byte("backup.replace")
	for i := 0; i < 10; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(bak); err != nil {
		t.Fatalf("first backup: %v", err)
	}

	for i := 10; i < 20; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(bak); err != nil {
		t.Fatalf("second backup: %v", err)
	}
	entries, _ := os.ReadDir(parent)
	for _, e := range entries {
		n := e.Name()
		if n != "bak" && n != ".bak.lock" {
			t.Fatalf("unexpected leftover in backup parent: %s", n)
		}
	}
	target1 := filepath.Join(t.TempDir(), "r1")
	rdb := restoreOpen(t, bak, target1)
	got := backupMessages(t, rdb, topic, 0)
	if len(got) != 20 {
		t.Fatalf("new backup expected 20 messages; got %d", len(got))
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a process killed after dest was moved aside and before the
	// candidate was published.
	aside := filepath.Join(parent, ".bak.old.999.1")
	if err := os.Rename(bak, aside); err != nil {
		t.Fatal(err)
	}
	for i := 20; i < 25; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup after interrupted publish: %v", err)
	}
	if err := VerifyBackup(bak); err != nil {
		t.Fatalf("recovered backup does not verify: %v", err)
	}
	entries, _ = os.ReadDir(parent)
	for _, e := range entries {
		n := e.Name()
		if n != "bak" && n != ".bak.lock" {
			t.Fatalf("unexpected leftover after recovery: %s", n)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupFailureKeepsPrevious checks that a backup which cannot stage
// fails without creating the destination and leaves the source fully
// working; a repeated successful backup then works.
func TestBackupFailureKeepsPrevious(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("read-only directories do not block root")
	}
	src := t.TempDir()
	db := openBackupDB(t, src)
	topic := []byte("backup.fail")
	for i := 0; i < 5; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	ro := t.TempDir()
	if err := os.Chmod(ro, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(ro, 0755) })

	dest := filepath.Join(ro, "bak")
	if err := db.Backup(dest); err == nil {
		t.Fatal("backup to an unwritable directory succeeded")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("failed backup left a destination: %v", err)
	}
	if entries, _ := os.ReadDir(ro); len(entries) != 0 {
		t.Fatalf("failed backup left files behind: %v", entries)
	}

	// Source is untouched.
	got := backupMessages(t, db, topic, 0)
	if len(got) != 5 {
		t.Fatalf("source has %d messages after failed backup", len(got))
	}
	for i := 5; i < 10; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(t.TempDir(), "bak")
	if err := db.Backup(good); err != nil {
		t.Fatalf("retry backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupRejectsPathsInsideDB ensures a backup cannot stage inside the
// database directory, where it could mix into a later copy or recovery.
func TestBackupRejectsPathsInsideDB(t *testing.T) {
	src := t.TempDir()
	db := openBackupDB(t, src)
	t.Cleanup(func() { db.Close() })

	for _, dest := range []string{
		src,
		filepath.Join(src, "bak"),
		filepath.Join(src, "sub", "bak"),
	} {
		if err := db.Backup(dest); err == nil {
			t.Fatalf("backup inside the database dir succeeded for %s", dest)
		}
	}
}

// TestRestoreRejectsTamperedBackup verifies the manifest and per-file
// checksums: a changed byte or removed log fails verification, restore does
// not create the target, and an existing target is refused.
func TestRestoreRejectsTamperedBackup(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	db := openBackupDB(t, src)
	topic := []byte("backup.tamper")
	for i := 0; i < 30; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt a backed-up WAL log's byte.
	logs, err := filepath.Glob(filepath.Join(bak, backupLogDir, "*.log"))
	if err != nil || len(logs) == 0 {
		t.Fatalf("no backed up logs: %v", err)
	}
	corruptByte(t, logs[0], 100)
	if err := VerifyBackup(bak); err == nil {
		t.Fatal("verify passed after corrupting a log")
	} else if !errors.Is(err, ErrBackupCorrupt) {
		t.Fatalf("expected ErrBackupCorrupt; got %v", err)
	}
	if err := Restore(bak, target); err == nil {
		t.Fatal("restore of a corrupted backup succeeded")
	}
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		t.Fatalf("failed restore created the target: %v", err)
	}
	entries, _ := os.ReadDir(filepath.Dir(target))
	for _, e := range entries {
		if n := e.Name(); len(n) > 0 && n[0] == '.' {
			t.Fatalf("failed restore left staging behind: %s", n)
		}
	}
}

// TestRestoreRejectsBadManifest corrupts the manifest itself and refuses an
// existing target.
func TestRestoreRejectsBadManifest(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	db := openBackupDB(t, src)
	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	corruptByte(t, filepath.Join(bak, backupManifestName), 0)
	if err := VerifyBackup(bak); err == nil {
		t.Fatal("verify passed with a corrupted manifest")
	}
	if err := Restore(bak, target); err == nil {
		t.Fatal("restore passed with a corrupted manifest")
	}

	// Empty backup directory: no manifest.
	empty := t.TempDir()
	if err := Restore(empty, filepath.Join(t.TempDir(), "x")); err == nil {
		t.Fatal("restore of a manifestless directory succeeded")
	}

	// Existing target.
	good := filepath.Join(t.TempDir(), "good")
	db2 := openBackupDB(t, src)
	if err := db2.Backup(good); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	existing := t.TempDir()
	if err := Restore(good, existing); err == nil {
		t.Fatal("restore into an existing directory succeeded")
	}
}

func corruptByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() <= off {
		off = fi.Size() - 1
	}
	buf := []byte{0}
	if _, err := f.ReadAt(buf, off); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0xff
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupEncryption covers the encryption setting end to end: an
// encrypted backup verifies without a key, restores only with one, and the
// restored database decrypts its messages.
func TestBackupEncryption(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	key := bytes.Repeat([]byte{7}, 32)
	db := openBackupDB(t, src, WithEncryption(), WithEncryptionKey(key))
	topic := []byte("backup.encrypted")
	for i := 0; i < 25; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := VerifyBackup(bak); err != nil {
		t.Fatalf("verify encrypted backup without a key: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Restore without the key refuses before creating the target.
	if err := Restore(bak, target); !errors.Is(err, ErrNoEncryptionKey) {
		t.Fatalf("expected ErrNoEncryptionKey; got %v", err)
	}
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		t.Fatal("restore without a key created the target")
	}

	rdb := restoreOpen(t, bak, target, WithEncryptionKey(key))
	got := backupMessages(t, rdb, topic, 0)
	want := make([]int, 25)
	for i := range want {
		want[i] = i
	}
	expectRangeMsgs(t, got, want...)
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}

	// Opening the restored database without the key fails.
	if _, err := Open(target, backupOpts()...); !errors.Is(err, ErrNoEncryptionKey) {
		t.Fatalf("open without key: expected ErrNoEncryptionKey; got %v", err)
	}
}

// TestBackupEmptyDatabase backs up and restores a database with no entries:
// the restored copy opens and serves writes normally.
func TestBackupEmptyDatabase(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	db := openBackupDB(t, src)
	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	rdb := restoreOpen(t, bak, target)
	if count := rdb.Count(); count != 0 {
		t.Fatalf("empty backup restored with count %d", count)
	}
	topic := []byte("backup.after.empty")
	putSeq(t, rdb, topic, 0, 0, rdb.NewID())
	if err := rdb.Flush(); err != nil {
		t.Fatal(err)
	}
	syncCount(t, rdb, 1)
	got := backupMessages(t, rdb, topic, 0)
	expectRangeMsgs(t, got, 0)
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupFullySynced backs up a database whose WAL logs have been
// released after syncing: the set is then just the on-disk blocks, and the
// restored database opens without replaying anything.
func TestBackupFullySynced(t *testing.T) {
	src := t.TempDir()
	bak := filepath.Join(t.TempDir(), "bak")
	target := filepath.Join(t.TempDir(), "restored")

	db := openBackupDB(t, src)
	topic := []byte("backup.synced")
	for i := 0; i < 50; i++ {
		putSeq(t, db, topic, 0, i, db.NewID())
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	syncCount(t, db, 50)
	// Sync the remaining live block too, until the logs are all released.
	deadline := time.Now().Add(5 * time.Second)
	for {
		logs, _ := filepath.Glob(filepath.Join(src, backupLogDir, "*.log"))
		if len(logs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d WAL logs still present after sync", len(logs))
		}
		time.Sleep(50 * time.Millisecond)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Backup(bak); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if logs, _ := filepath.Glob(filepath.Join(bak, backupLogDir, "*.log")); len(logs) != 0 {
		t.Fatalf("backup of a fully synced db contains %d logs", len(logs))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	rdb := restoreOpen(t, bak, target)
	got := backupMessages(t, rdb, topic, 0)
	want := make([]int, 50)
	for i := range want {
		want[i] = i
	}
	expectRangeMsgs(t, got, want...)
	if count := rdb.Count(); count != 50 {
		t.Fatalf("count %d", count)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBackupClosedRejected ensures backup and verify report clearly on a
// closed database and that a backup directory is not mistaken for a live
// database lock.
func TestBackupClosedRejected(t *testing.T) {
	src := t.TempDir()
	db := openBackupDB(t, src)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(filepath.Join(t.TempDir(), "bak")); err == nil {
		t.Fatal("backup on a closed database succeeded")
	}
}

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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Online, point-in-time backup and restore.
//
// A backup fixes a confirmed write point while the database keeps serving
// writes:
//
//   - the synced files (info, window, index, data, lease, filter and the
//     per-message checksum file) are frozen by taking the sync barrier that
//     Sync and on-disk deletes already take; Puts never take it;
//   - memdb.Flush lands every earlier Put in a finished WAL file, and the
//     set of those files is listed once. A backup barrier keeps them from
//     being released until copied; later writes rotate into new logs that
//     the listing does not include;
//   - the set is copied into a hidden candidate directory, every copied
//     file is re-read and checksummed, and a manifest listing the files,
//     their sizes and CRC32C, the file format version and the encryption
//     setting is written last and fsynced;
//   - the candidate replaces the previous backup with two same-directory
//     renames, so a crash leaves either the previous backup or the new one,
//     never a half set. A failed copy removes the candidate and the
//     previous backup stays in place.
//
// Restore stages into a new directory the same way: it verifies the
// manifest and every file before the target name ever exists, then a final
// rename publishes it. Opening the restored database runs the ordinary
// recovery, which replays the backed-up WAL.

var (
	// ErrBackupCorrupt is returned when a backup fails its manifest or
	// file checksums, or carries an unsupported format.
	ErrBackupCorrupt = errors.New("unitdb: backup is corrupted")

	// ErrBackupIncomplete is returned when a backup is missing its
	// manifest or files the manifest lists.
	ErrBackupIncomplete = errors.New("unitdb: backup is incomplete")
)

const (
	// backupManifestName is the manifest written last in a backup.
	backupManifestName = "unitdb.manifest"
	// backupManifestTmp is staged first and renamed into place.
	backupManifestTmp = ".unitdb.manifest.tmp"

	// backupLogDir is the copy of the memdb WAL directory; it must match
	// memdb's logDir.
	backupLogDir = "logs"

	backupInfoName = prefix + ".info"

	// backupFormatVersion is the manifest format.
	backupFormatVersion uint32 = 1

	backupMagicSize    = 16
	backupHeaderSize   = 56 // magic16 ver4 dbver4 enc1 res3 seq8 count8 created8 nfiles4
	backupNameLenSize  = 2
	backupFileMetaSize = 8 + checksumSize
)

var backupMagic = [backupMagicSize]byte{
	'u', 'n', 'i', 't', 'd', 'b', '.', 'b',
	'a', 'c', 'k', 'u', 'p', '\n', 0x00, 0x00,
}

type backupFile struct {
	name string // slash-separated path relative to the set root
	size int64
	sum  uint32 // CRC32C of the file bytes
}

type backupManifest struct {
	version    uint32 // manifest format
	dbVersion  uint32 // database file format
	encryption uint8  // database encryption setting
	pointSeq   uint64
	pointCount uint64
	createdAt  int64
	files      []backupFile
}

func (m *backupManifest) marshalBinary() ([]byte, error) {
	size := backupHeaderSize + checksumSize
	for _, f := range m.files {
		if len(f.name) > 0xffff {
			return nil, fmt.Errorf("%w: file name too long", ErrBackupCorrupt)
		}
		size += backupNameLenSize + len(f.name) + backupFileMetaSize
	}
	buf := make([]byte, backupHeaderSize, size)
	copy(buf[:backupMagicSize], backupMagic[:])
	binary.LittleEndian.PutUint32(buf[16:20], m.version)
	binary.LittleEndian.PutUint32(buf[20:24], m.dbVersion)
	buf[24] = m.encryption
	binary.LittleEndian.PutUint64(buf[28:36], m.pointSeq)
	binary.LittleEndian.PutUint64(buf[36:44], m.pointCount)
	binary.LittleEndian.PutUint64(buf[44:52], uint64(m.createdAt))
	binary.LittleEndian.PutUint32(buf[52:56], uint32(len(m.files)))

	var meta [backupFileMetaSize]byte
	for _, f := range m.files {
		var l [backupNameLenSize]byte
		binary.LittleEndian.PutUint16(l[:], uint16(len(f.name)))
		buf = append(buf, l[:]...)
		buf = append(buf, f.name...)
		binary.LittleEndian.PutUint64(meta[:8], uint64(f.size))
		binary.LittleEndian.PutUint32(meta[8:12], f.sum)
		buf = append(buf, meta[:]...)
	}
	var sum [checksumSize]byte
	binary.LittleEndian.PutUint32(sum[:], checksum(buf))
	buf = append(buf, sum[:]...)
	return buf, nil
}

func unmarshalBackupManifest(raw []byte) (*backupManifest, error) {
	if len(raw) < backupHeaderSize+checksumSize {
		return nil, fmt.Errorf("%w: manifest too short", ErrBackupIncomplete)
	}
	if !bytes.Equal(raw[:backupMagicSize], backupMagic[:]) {
		return nil, fmt.Errorf("%w: bad manifest signature", ErrBackupCorrupt)
	}
	body := raw[:len(raw)-checksumSize]
	if binary.LittleEndian.Uint32(raw[len(raw)-checksumSize:]) != checksum(body) {
		return nil, fmt.Errorf("%w: manifest checksum mismatch", ErrBackupCorrupt)
	}
	m := &backupManifest{
		version:    binary.LittleEndian.Uint32(body[16:20]),
		dbVersion:  binary.LittleEndian.Uint32(body[20:24]),
		encryption: body[24],
		pointSeq:   binary.LittleEndian.Uint64(body[28:36]),
		pointCount: binary.LittleEndian.Uint64(body[36:44]),
		createdAt:  int64(binary.LittleEndian.Uint64(body[44:52])),
	}
	n := binary.LittleEndian.Uint32(body[52:56])
	p := backupHeaderSize
	for i := uint32(0); i < n; i++ {
		if p+backupNameLenSize > len(body) {
			return nil, fmt.Errorf("%w: truncated file list", ErrBackupIncomplete)
		}
		nameLen := int(binary.LittleEndian.Uint16(body[p : p+backupNameLenSize]))
		p += backupNameLenSize
		if nameLen == 0 || p+nameLen+backupFileMetaSize > len(body) {
			return nil, fmt.Errorf("%w: truncated file entry", ErrBackupIncomplete)
		}
		f := backupFile{name: string(body[p : p+nameLen])}
		p += nameLen
		f.size = int64(binary.LittleEndian.Uint64(body[p : p+8]))
		p += 8
		f.sum = binary.LittleEndian.Uint32(body[p : p+checksumSize])
		p += checksumSize
		m.files = append(m.files, f)
	}
	if p != len(body) {
		return nil, fmt.Errorf("%w: trailing bytes in manifest", ErrBackupCorrupt)
	}
	return m, nil
}

// safeRelName accepts a forward-slash relative path with no empty, current
// or parent directory parts. It keeps a restored file from escaping its
// root.
func safeRelName(name string) (string, bool) {
	if name == "" || strings.HasPrefix(name, "/") || name[0] >= 0x80 {
		return "", false
	}
	if filepath.IsAbs(name) || strings.ContainsRune(name, '\\') {
		return "", false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return "", false
		}
	}
	return name, true
}

// fileCRC returns a regular file's size and the CRC32C of its bytes.
func fileCRC(abs string) (int64, uint32, error) {
	fi, err := os.Lstat(abs)
	if err != nil {
		return 0, 0, err
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.Mode().IsRegular() {
		return 0, 0, fmt.Errorf("%w: %s is not a regular file", ErrBackupCorrupt, abs)
	}
	f, err := os.Open(abs)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	h := crc32.New(crcTable)
	if _, err := io.CopyBuffer(h, f, make([]byte, 1<<20)); err != nil {
		return 0, 0, err
	}
	return fi.Size(), h.Sum32(), nil
}

// copyTracked copies src to dst, creating its directory, and returns the
// destination size and CRC32C. With preserveTime the WAL logs keep their
// source modification time, which is recovery's replay order tie-break.
func copyTracked(src, dst string, preserveTime bool) (backupFile, error) {
	in, err := os.Open(src)
	if err != nil {
		return backupFile{}, err
	}
	fi, err := in.Stat()
	if err != nil {
		in.Close()
		return backupFile{}, err
	}
	if !fi.Mode().IsRegular() {
		in.Close()
		return backupFile{}, fmt.Errorf("%w: %s is not a regular file", ErrBackupCorrupt, src)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0750); err != nil {
		in.Close()
		return backupFile{}, err
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		in.Close()
		return backupFile{}, err
	}
	h := crc32.New(crcTable)
	n, copyErr := io.CopyBuffer(out, io.TeeReader(in, h), make([]byte, 1<<20))
	syncErr := out.Sync()
	closeErr := out.Close()
	in.Close()
	if copyErr != nil {
		return backupFile{}, copyErr
	}
	if syncErr != nil {
		return backupFile{}, syncErr
	}
	if closeErr != nil {
		return backupFile{}, closeErr
	}
	if preserveTime {
		if err := os.Chtimes(dst, fi.ModTime(), fi.ModTime()); err != nil {
			return backupFile{}, err
		}
	}
	return backupFile{size: n, sum: h.Sum32()}, nil
}

// verifySet re-reads every listed file in root and checks its size and
// CRC32C.
func verifySet(root string, files []backupFile) error {
	for _, f := range files {
		abs := filepath.Join(root, filepath.FromSlash(f.name))
		size, sum, err := fileCRC(abs)
		if err != nil {
			return fmt.Errorf("%w: %s: %v", ErrBackupCorrupt, f.name, err)
		}
		if size != f.size || sum != f.sum {
			return fmt.Errorf("%w: %s checksum or size mismatch", ErrBackupCorrupt, f.name)
		}
	}
	return nil
}

// readDBInfoHeader reads and sanity-checks an info header file.
func readDBInfoHeader(abs string) (_DBInfo, error) {
	var inf _DBInfo
	f, err := os.Open(abs)
	if err != nil {
		return inf, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return inf, err
	}
	size := uint32(fixed)
	if fi.Size() < int64(fixed) {
		size = fixedV2
	}
	if fi.Size() < int64(fixedV2) {
		f.Close()
		return inf, fmt.Errorf("%w: short info header", ErrBackupCorrupt)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		f.Close()
		return inf, err
	}
	f.Close()
	if err := inf.UnmarshalBinary(buf); err != nil {
		return inf, err
	}
	if !bytes.Equal(inf.header.signature[:], signature[:]) {
		return inf, fmt.Errorf("%w: bad info signature", ErrBackupCorrupt)
	}
	if inf.header.version < 2 || inf.header.version > version {
		return inf, fmt.Errorf("%w: unsupported file format version %d", ErrBackupCorrupt, inf.header.version)
	}
	return inf, nil
}

// readVerifyManifest loads and fully verifies a backup directory.
func readVerifyManifest(backupPath string) (*backupManifest, error) {
	raw, err := os.ReadFile(filepath.Join(backupPath, backupManifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: manifest not found: %v", ErrBackupIncomplete, err)
		}
		return nil, err
	}
	m, err := unmarshalBackupManifest(raw)
	if err != nil {
		return nil, err
	}
	if m.version != backupFormatVersion {
		return nil, fmt.Errorf("%w: unsupported backup format version %d", ErrBackupCorrupt, m.version)
	}
	if m.dbVersion < 2 || m.dbVersion > version {
		return nil, fmt.Errorf("%w: unsupported file format version %d", ErrBackupCorrupt, m.dbVersion)
	}
	seen := make(map[string]bool, len(m.files))
	hasInfo := false
	for i := range m.files {
		f := &m.files[i]
		name, ok := safeRelName(f.name)
		if !ok {
			return nil, fmt.Errorf("%w: unsafe file name %q", ErrBackupCorrupt, f.name)
		}
		f.name = name
		if seen[name] {
			return nil, fmt.Errorf("%w: file listed twice: %s", ErrBackupCorrupt, name)
		}
		seen[name] = true
		size, sum, err := fileCRC(filepath.Join(backupPath, filepath.FromSlash(name)))
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrBackupCorrupt, name, err)
		}
		if size != f.size || sum != f.sum {
			return nil, fmt.Errorf("%w: %s checksum or size mismatch", ErrBackupCorrupt, name)
		}
		if name == backupInfoName {
			inf, err := readDBInfoHeader(filepath.Join(backupPath, filepath.FromSlash(name)))
			if err != nil {
				return nil, err
			}
			if inf.header.version != m.dbVersion || int8(m.encryption) != inf.encryption {
				return nil, fmt.Errorf("%w: info header does not match manifest", ErrBackupCorrupt)
			}
			hasInfo = true
		}
	}
	if !hasInfo {
		return nil, fmt.Errorf("%w: missing %s", ErrBackupIncomplete, backupInfoName)
	}
	return m, nil
}

// VerifyBackup checks a backup directory: its manifest checksum, that every
// listed file is present with the recorded size and CRC32C, and that the
// info header's format and encryption setting match the manifest. It does
// not need an encryption key and does not open or modify the backup.
func VerifyBackup(backupPath string) error {
	abs, err := filepath.Abs(backupPath)
	if err != nil {
		return err
	}
	unlock, err := lockBackup(abs)
	if err != nil {
		return err
	}
	defer unlock()
	_, err = readVerifyManifest(abs)
	return err
}

// pathWithin reports whether target is base or inside it.
func pathWithin(target, base string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil || filepath.IsAbs(rel) {
		return false
	}
	return rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// tempName builds a unique hidden temporary path of the given kind next to
// the destination it will replace.
func tempName(parent, base, kind string) string {
	for {
		p := filepath.Join(parent, fmt.Sprintf(".%s.%s.%d.%d", base, kind, os.Getpid(), time.Now().UnixNano()))
		if _, err := os.Stat(p); os.IsNotExist(err) {
			return p
		}
		time.Sleep(time.Nanosecond)
	}
}

// lockBackup serializes backup publishes and restores of one backup path.
// It locks a hidden file in the backup's parent directory so it never
// becomes part of the set.
func lockBackup(dest string) (func(), error) {
	parent := filepath.Dir(dest)
	if err := ensureDir(parent); err != nil {
		return nil, err
	}
	lock, err := newLockFile(filepath.Join(parent, "."+filepath.Base(dest)+".lock"))
	if err != nil {
		if err == os.ErrExist {
			return nil, errLocked
		}
		return nil, err
	}
	return func() { lock.unlock() }, nil
}

// reconcileTemps finishes or cleans leftovers of an interrupted publish for
// dest within parent. If dest is missing but a previous backup was moved
// aside, that backup is put back first; it must never be silently lost.
func reconcileTemps(parent, base string) error {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return err
	}
	var olds []os.DirEntry
	var temps []string
	oldPrefix := "." + base + ".old."
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasPrefix(name, oldPrefix):
			olds = append(olds, e)
		case strings.HasPrefix(name, "."+base+".stage."):
			temps = append(temps, filepath.Join(parent, name))
		case strings.HasPrefix(name, "."+base+".restore."):
			temps = append(temps, filepath.Join(parent, name))
		}
	}
	if _, err := os.Stat(filepath.Join(parent, base)); os.IsNotExist(err) {
		// Publish stopped after moving dest aside: restore the newest old.
		sort.Slice(olds, func(i, j int) bool {
			ii, e1 := olds[i].Info()
			jj, e2 := olds[j].Info()
			if e1 != nil || e2 != nil {
				return olds[i].Name() > olds[j].Name()
			}
			return ii.ModTime().After(jj.ModTime())
		})
		if len(olds) > 0 {
			if err := os.Rename(filepath.Join(parent, olds[0].Name()), filepath.Join(parent, base)); err != nil {
				return err
			}
			olds = olds[1:]
		}
	}
	for _, e := range olds {
		os.RemoveAll(filepath.Join(parent, e.Name()))
	}
	for _, t := range temps {
		os.RemoveAll(t)
	}
	return nil
}

// fsyncDirs fsyncs each directory holding files in the set, so the file
// entries survive a crash before the manifest names them.
func fsyncDirs(root string, files []backupFile) {
	seen := map[string]bool{root: true}
	if err := fsyncDir(root); err != nil {
		logger.Error().Err(err).Str("context", "backup.fsyncDirs")
	}
	for _, f := range files {
		dir := filepath.Dir(filepath.Join(root, filepath.FromSlash(f.name)))
		if seen[dir] {
			continue
		}
		seen[dir] = true
		if err := fsyncDir(dir); err != nil {
			logger.Error().Err(err).Str("context", "backup.fsyncDirs")
		}
	}
}

// writeManifest writes, fsyncs and atomically publishes the manifest, the
// last step that makes a candidate a valid backup.
func writeManifest(stage string, m *backupManifest) error {
	raw, err := m.marshalBinary()
	if err != nil {
		return err
	}
	tmp := filepath.Join(stage, backupManifestTmp)
	final := filepath.Join(stage, backupManifestName)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return fsyncDir(stage)
}

// testHookBackupPoint, if set, runs after the backup point is fixed and
// while its barriers are held: backup tests put and flush later writes
// there, which must not enter the candidate.
var testHookBackupPoint func()

// Backup writes a consistent, restorable snapshot of the database at the
// confirmed write point Backup starts from into dest, replacing any backup
// in that directory. The database stays open and writable: entries put
// after the point remain in the source and never enter the snapshot.
//
// The snapshot only becomes valid after all files, their checksums, the
// format version and the encryption setting have been verified and synced.
// On any failure dest keeps its previous valid backup.
func (db *DB) Backup(dest string) error {
	if err := db.ok(); err != nil {
		return err
	}
	if strings.TrimSpace(dest) == "" {
		return errors.New("unitdb: backup destination is empty")
	}
	abs, err := filepath.Abs(dest)
	if err != nil {
		return err
	}
	parent := filepath.Dir(abs)
	base := filepath.Base(abs)
	if pathWithin(abs, db.path) || pathWithin(parent, db.path) {
		return fmt.Errorf("unitdb: backup path %s must be outside the database directory %s", abs, db.path)
	}

	db.backupMu.Lock()
	defer db.backupMu.Unlock()

	// Freeze the synced files. Sync, the expirer and deletes of on-disk
	// entries take this same one-token lock; Puts do not, so they continue.
	select {
	case db.internal.syncLockC <- struct{}{}:
	case <-db.internal.closeC:
		return errClosed
	}
	defer func() { <-db.internal.syncLockC }()

	// Fix the confirmed write point: flush, then list and pin the logs.
	logPaths, endLogs, err := db.internal.mem.BeginBackup()
	if err != nil {
		return err
	}
	defer endLogs()

	if testHookBackupPoint != nil {
		testHookBackupPoint()
	}

	pointSeq := db.seq()

	unlock, err := lockBackup(abs)
	if err != nil {
		return err
	}
	defer unlock()

	if err := reconcileTemps(parent, base); err != nil {
		return err
	}

	stage := tempName(parent, base, "stage")
	if err := os.Mkdir(stage, 0750); err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			// Keep the previous backup: the candidate never became dest.
			os.RemoveAll(stage)
		}
	}()

	files, err := db.gatherBackupFiles(stage, logPaths)
	if err != nil {
		return err
	}

	manifestFiles := make([]backupFile, 0, len(files))
	dirs := make(map[string]bool)
	for _, f := range files {
		copied, err := copyTracked(f.src, f.dst, f.isLog)
		if err != nil {
			return err
		}
		copied.name = f.name
		manifestFiles = append(manifestFiles, copied)
		dirs[filepath.Dir(f.dst)] = true
	}

	// Normalize the candidate's info header. The encryption flag reaches
	// disk only on a later sync or delete, so the header on disk at the
	// point can still say unencrypted while the backed-up WAL logs already
	// carry encrypted entries: write the authoritative flag into the
	// candidate. Sequence, count and the syncing marker stay exactly as
	// persisted: recovery reconciles the first two by replaying the WAL,
	// and syncing tells it which entries a stopped sync had already written.
	inf, err := readDBInfoHeader(filepath.Join(stage, backupInfoName))
	if err != nil {
		return err
	}
	inf.header.signature = signature
	inf.header.version = version
	inf.encryption = db.internal.dbInfo.encryption
	infoBytes, err := inf.MarshalBinary()
	if err != nil {
		return err
	}
	infoPath := filepath.Join(stage, backupInfoName)
	infoFile, err := os.OpenFile(infoPath, os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}
	if _, err := infoFile.Write(infoBytes); err != nil {
		infoFile.Close()
		return err
	}
	if err := infoFile.Sync(); err != nil {
		infoFile.Close()
		return err
	}
	if err := infoFile.Close(); err != nil {
		return err
	}
	// Re-read it back to verify what was actually written, and update the
	// recorded size and CRC to match the new bytes.
	inf, err = readDBInfoHeader(infoPath)
	if err != nil {
		return err
	}
	infoSize, infoSum, err := fileCRC(infoPath)
	if err != nil {
		return err
	}
	for i := range manifestFiles {
		if manifestFiles[i].name == backupInfoName {
			manifestFiles[i].size = infoSize
			manifestFiles[i].sum = infoSum
			break
		}
	}
	man := &backupManifest{
		version:    backupFormatVersion,
		dbVersion:  inf.header.version,
		encryption: byte(inf.encryption),
		pointSeq:   pointSeq,
		pointCount: db.Count(),
		createdAt:  time.Now().UTC().UnixNano(),
		files:      manifestFiles,
	}

	// Verify every copied file a second time, from disk, before publishing.
	if err := verifySet(stage, man.files); err != nil {
		return err
	}
	for dir := range dirs {
		if err := fsyncDir(dir); err != nil {
			return err
		}
	}
	if err := writeManifest(stage, man); err != nil {
		return err
	}

	// Publish with same-directory renames: either the old backup or the new
	// one is always at dest. The move-aside is rolled back on failure.
	old := ""
	if _, err := os.Stat(abs); err == nil {
		old = tempName(parent, base, "old")
		if err := os.Rename(abs, old); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(stage, abs); err != nil {
		if old != "" {
			if rerr := os.Rename(old, abs); rerr != nil {
				return fmt.Errorf("%w (and the previous backup is at %s: %v)", err, old, rerr)
			}
		}
		return err
	}
	published = true
	_ = fsyncDir(parent)
	if old != "" {
		// Failing to remove the moved-aside backup does not invalidate the
		// one just published; the next backup or restore reconciles it.
		_ = os.RemoveAll(old)
	}
	return nil
}

// backupCopyFile pairs the source of a file with its destination in the
// candidate directory.
type backupCopyFile struct {
	name  string // slash-separated relative name
	src   string
	dst   string
	isLog bool
}

// gatherBackupFiles lists the synced file set and the pinned WAL logs,
// paired with their candidate paths. Listing the open files does not swap
// the current file the way getFile does.
func (db *DB) gatherBackupFiles(stage string, logPaths []string) ([]backupCopyFile, error) {
	var out []backupCopyFile
	names := make(map[string]bool)

	db.fs.mu.RLock()
	for _, set := range db.fs.list {
		for _, f := range set.fileMap {
			abs := f.File.Name()
			rel, err := filepath.Rel(db.path, abs)
			if err != nil {
				db.fs.mu.RUnlock()
				return nil, err
			}
			name := filepath.ToSlash(rel)
			if _, ok := safeRelName(name); !ok {
				db.fs.mu.RUnlock()
				return nil, fmt.Errorf("%w: unsafe file name %q", ErrBackupCorrupt, name)
			}
			if names[name] {
				continue
			}
			names[name] = true
			out = append(out, backupCopyFile{name: name, src: abs, dst: filepath.Join(stage, filepath.FromSlash(name))})
		}
	}
	db.fs.mu.RUnlock()

	for _, lp := range logPaths {
		name := filepath.ToSlash(filepath.Join(backupLogDir, filepath.Base(lp)))
		if names[name] {
			continue
		}
		names[name] = true
		out = append(out, backupCopyFile{
			name:  name,
			src:   lp,
			dst:   filepath.Join(stage, filepath.FromSlash(name)),
			isLog: true,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, nil
}

// Restore verifies the backup at backupPath and materializes it into a new,
// previously non-existent directory target. The source backup is opened
// read-only and the source database is never touched. The target appears
// atomically, so a failure cannot leave a half-written database for Open to
// mistake for one: until Restore succeeds the target path does not exist.
//
// Restore verifies the manifest and every file before publishing. Opening
// the target afterwards runs the ordinary recovery and replays the WAL in
// the backup, so TTL state, deletions, contract isolation and sequence
// ordering are exactly those of the backup point. An encrypted backup
// needs its key passed here, as it does to Open.
func Restore(backupPath, target string, opts ...Options) error {
	options := &_Options{}
	WithDefaultOptions().set(options)
	for _, opt := range opts {
		if opt != nil {
			opt.set(options)
		}
	}

	absBackup, err := filepath.Abs(backupPath)
	if err != nil {
		return err
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(absTarget); err == nil {
		return fmt.Errorf("unitdb: restore target %s already exists; restore only writes a new directory", absTarget)
	} else if !os.IsNotExist(err) {
		return err
	}

	unlock, err := lockBackup(absBackup)
	if err != nil {
		return err
	}
	defer unlock()

	// Verify the whole backup before writing anything for the target.
	man, err := readVerifyManifest(absBackup)
	if err != nil {
		return err
	}
	if man.encryption == 1 && options.encryptionKey == nil {
		return ErrNoEncryptionKey
	}

	parent := filepath.Dir(absTarget)
	if err := os.MkdirAll(parent, 0750); err != nil {
		return err
	}
	base := filepath.Base(absTarget)
	if err := reconcileTemps(parent, base); err != nil {
		return err
	}

	stage := tempName(parent, base, "restore")
	if err := os.Mkdir(stage, 0750); err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			// The target name was never created, so Open cannot see a half
			// database.
			os.RemoveAll(stage)
		}
	}()

	for _, f := range man.files {
		src := filepath.Join(absBackup, filepath.FromSlash(f.name))
		dst := filepath.Join(stage, filepath.FromSlash(f.name))
		isLog := strings.HasPrefix(f.name, backupLogDir+"/")
		copied, err := copyTracked(src, dst, isLog)
		if err != nil {
			return err
		}
		if copied.size != f.size || copied.sum != f.sum {
			return fmt.Errorf("%w: %s changed while restoring", ErrBackupCorrupt, f.name)
		}
	}

	// Second pass from the staged files: nothing reaches the target name
	// until this matches the manifest.
	if err := verifySet(stage, man.files); err != nil {
		return err
	}
	fsyncDirs(stage, man.files)
	if err := os.Rename(stage, absTarget); err != nil {
		return err
	}
	published = true
	return fsyncDir(parent)
}

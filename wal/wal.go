// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal/walpb"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

const (
	// Record 实例类型
	// 该类型日志记录的 Data 字段中保存的是元数据
	// 数据类型定义在 etcdserver/etcdserverpb/etcdserverpb.pb.go 文件的 Metadata 结构体中
	metadataType int64 = iota + 1
	// 该类型日志记录的 Data 字段中保存的是 Entry 记录
	entryType
	// 该类型日志记录的 Data 字段中保存了当前集群的状态信息（HardState）
	stateType
	// 该类型的日志记录主要用于数据校验
	crcType
	// 该类型的日志记录中保存了快照数据的相关信息（即 walpb.Snapshot）
	snapshotType

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	warnSyncDuration = time.Second
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "wal")

	ErrMetadataConflict = errors.New("wal: conflicting metadata found")
	ErrFileNotFound     = errors.New("wal: file not found")
	ErrCRCMismatch      = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound = errors.New("wal: snapshot not found")
	crcTable            = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
	lg *zap.Logger

	dir string // the living directory of the underlay files

	// dirFile is a fd for the wal directory for syncing on Rename
	// 根据 dir 路径创建的 File 实例。
	dirFile *os.File

	// 每个 WAL 日志文件的头部都会写入 metadata 元数据
	metadata []byte // metadata recorded at the head of each WAL
	// 在 HardState 中记录了当前的任期、当前节点的投票结果和己提交日志的位置。
	// WAL 日志记录是批量追加的，在每次批量写入 entryType 类型的日志之后，都会再追加一条 stateType 类型的日志记录，
	state raftpb.HardState // hardstate recorded at the head of WAL

	// walpb.Snapshot 中的 Index 字段记录了对应快照数据所涵盖的最后一条 Entry 记录的索引值，Term 字段则记录了对应 Entry 记录的 Term 值。
	// 每次读取 WAL 日志时，并不会每次都从头开始读取，而是通过 start 宇段指定具体的起始位置。
	start walpb.Snapshot // snapshot to start reading
	// 读取 WAL 日志文件时，将二进制数据反序列化成 Record
	decoder   *decoder     // decoder to decode records
	readClose func() error // closer for decode reader

	// 读写 WAL 日志时需要加锁同步
	mu sync.Mutex
	// WAL 中最后一条 Entry 记录的索引值
	enti    uint64   // index of the last entry saved to the wal
	encoder *encoder // encoder to encode records

	// 当前 WAL 实例管理的所有 WAL 文件的句柄
	locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)
	// filePipeline 实例负责创建新的临时文件并为日志文件预分配空间
	// 当进行日志文件切换时会直接对临时文件进行重命名
	fp *filePipeline
}

// Create creates a WAL ready for appending records. The given metadata is
// recorded at the head of each WAL file, and can be retrieved with ReadAll.
func Create(lg *zap.Logger, dirpath string, metadata []byte) (*WAL, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	// keep temporary wal directory so WAL initialization appears atomic
	// 创建临时文件
	tmpdirpath := filepath.Clean(dirpath) + ".tmp"
	if fileutil.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		if lg != nil {
			lg.Warn(
				"failed to create a temporary WAL directory",
				zap.String("tmp-dir-path", tmpdirpath),
				zap.String("dir-path", dirpath),
				zap.Error(err),
			)
		}
		return nil, err
	}

	p := filepath.Join(tmpdirpath, walName(0, 0))
	// 对文件上互斥锁
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)
	if err != nil {
		if lg != nil {
			lg.Warn(
				"failed to flock an initial WAL file",
				zap.String("path", p),
				zap.Error(err),
			)
		}
		return nil, err
	}
	// 定位到文件末尾
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		if lg != nil {
			lg.Warn(
				"failed to seek an initial WAL file",
				zap.String("path", p),
				zap.Error(err),
			)
		}
		return nil, err
	}
	// 预分配文件空间，大小为 64MB
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
		if lg != nil {
			lg.Warn(
				"failed to preallocate an initial WAL file",
				zap.String("path", p),
				zap.Int64("segment-bytes", SegmentSizeBytes),
				zap.Error(err),
			)
		}
		return nil, err
	}

	w := &WAL{
		lg:       lg,
		dir:      dirpath,
		metadata: metadata,
	}
	// 对 WAL 文件创建一个 encoder
	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	// 将 FD 添加到 locks 数组中
	w.locks = append(w.locks, f)
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}
	// 将 metadataType 类型的 Record 记录在 WAL 文件的头部
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	// 随后写入一条空的 snapshotType Record
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
		return nil, err
	}

	// 初始化完成之后重命名文件
	if w, err = w.renameWAL(tmpdirpath); err != nil {
		if lg != nil {
			lg.Warn(
				"failed to rename the temporary WAL directory",
				zap.String("tmp-dir-path", tmpdirpath),
				zap.String("dir-path", w.dir),
				zap.Error(err),
			)
		}
		return nil, err
	}

	var perr error
	defer func() {
		if perr != nil {
			w.cleanupWAL(lg)
		}
	}()

	// directory was renamed; sync parent dir to persist rename
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	if perr != nil {
		if lg != nil {
			lg.Warn(
				"failed to open the parent data directory",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
		}
		return nil, perr
	}
	start := time.Now()
	// 同步上述文件操作
	if perr = fileutil.Fsync(pdir); perr != nil {
		if lg != nil {
			lg.Warn(
				"failed to fsync the parent data directory file",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
		}
		return nil, perr
	}
	walFsyncSec.Observe(time.Since(start).Seconds())

	// 关闭目录
	if perr = pdir.Close(); perr != nil {
		if lg != nil {
			lg.Warn(
				"failed to close the parent data directory file",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
		}
		return nil, perr
	}

	return w, nil
}

func (w *WAL) cleanupWAL(lg *zap.Logger) {
	var err error
	if err = w.Close(); err != nil {
		if lg != nil {
			lg.Panic("failed to close WAL during cleanup", zap.Error(err))
		} else {
			plog.Panicf("failed to close WAL during cleanup: %v", err)
		}
	}
	brokenDirName := fmt.Sprintf("%s.broken.%v", w.dir, time.Now().Format("20060102.150405.999999"))
	if err = os.Rename(w.dir, brokenDirName); err != nil {
		if lg != nil {
			lg.Panic(
				"failed to rename WAL during cleanup",
				zap.Error(err),
				zap.String("source-path", w.dir),
				zap.String("rename-path", brokenDirName),
			)
		} else {
			plog.Panicf("failed to rename WAL during cleanup: %v", err)
		}
	}
}

func (w *WAL) renameWAL(tmpdirpath string) (*WAL, error) {
	if err := os.RemoveAll(w.dir); err != nil {
		return nil, err
	}
	// On non-Windows platforms, hold the lock while renaming. Releasing
	// the lock and trying to reacquire it quickly can be flaky because
	// it's possible the process will fork to spawn a process while this is
	// happening. The fds are set up as close-on-exec by the Go runtime,
	// but there is a window between the fork and the exec where another
	// process holds the lock.
	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		if _, ok := err.(*os.LinkError); ok {
			return w.renameWALUnlock(tmpdirpath)
		}
		return nil, err
	}
	w.fp = newFilePipeline(w.lg, w.dir, SegmentSizeBytes)
	df, err := fileutil.OpenDir(w.dir)
	w.dirFile = df
	return w, err
}

func (w *WAL) renameWALUnlock(tmpdirpath string) (*WAL, error) {
	// rename of directory with locked files doesn't work on windows/cifs;
	// close the WAL to release the locks so the directory can be renamed.
	if w.lg != nil {
		w.lg.Info(
			"closing WAL to release flock and retry directory renaming",
			zap.String("from", tmpdirpath),
			zap.String("to", w.dir),
		)
	} else {
		plog.Infof("releasing file lock to rename %q to %q", tmpdirpath, w.dir)
	}
	w.Close()

	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}

	// reopen and relock
	newWAL, oerr := Open(w.lg, w.dir, walpb.Snapshot{})
	if oerr != nil {
		return nil, oerr
	}
	if _, _, _, err := newWAL.ReadAll(); err != nil {
		newWAL.Close()
		return nil, err
	}
	return newWAL, nil
}

// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
func Open(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(lg, dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(lg, dirpath, snap, false)
}

func openAtIndex(lg *zap.Logger, dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	names, nameIndex, err := selectWALFiles(lg, dirpath, snap)
	if err != nil {
		return nil, err
	}

	rs, ls, closer, err := openWALFiles(lg, dirpath, names, nameIndex, write)
	if err != nil {
		return nil, err
	}

	// create a WAL ready for reading
	w := &WAL{
		lg:        lg,
		dir:       dirpath,
		start:     snap,
		decoder:   newDecoder(rs...),
		readClose: closer,
		locks:     ls,
	}

	if write {
		// write reuses the file descriptors from read; don't close so
		// WAL can append without dropping the file lock
		w.readClose = nil
		if _, _, err := parseWALName(filepath.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		w.fp = newFilePipeline(lg, w.dir, SegmentSizeBytes)
	}

	return w, nil
}

func selectWALFiles(lg *zap.Logger, dirpath string, snap walpb.Snapshot) ([]string, int, error) {
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, -1, err
	}

	nameIndex, ok := searchIndex(lg, names, snap.Index)
	if !ok || !isValidSeq(lg, names[nameIndex:]) {
		err = ErrFileNotFound
		return nil, -1, err
	}

	return names, nameIndex, nil
}

func openWALFiles(lg *zap.Logger, dirpath string, names []string, nameIndex int, write bool) ([]io.Reader, []*fileutil.LockedFile, func() error, error) {
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	ls := make([]*fileutil.LockedFile, 0)
	for _, name := range names[nameIndex:] {
		p := filepath.Join(dirpath, name)
		if write {
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, l)
			rcs = append(rcs, l)
		} else {
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, nil)
			rcs = append(rcs, rf)
		}
		rs = append(rs, rcs[len(rcs)-1])
	}

	closer := func() error { return closeAll(rcs...) }

	return rs, ls, closer, nil
}

// ReadAll reads out records of the current WAL.
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// If opened in read mode, it will try to read all records if possible.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records.
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{}
	decoder := w.decoder

	var match bool
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType:
			e := mustUnmarshalEntry(rec.Data)
			if e.Index > w.start.Index {
				ents = append(ents[:e.Index-w.start.Index-1], e)
			}
			w.enti = e.Index

		case stateType:
			state = mustUnmarshalState(rec.Data)

		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			metadata = rec.Data

		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				return nil, state, nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)

		case snapshotType:
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data)
			if snap.Index == w.start.Index {
				if snap.Term != w.start.Term {
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
				match = true
			}

		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	switch w.tail() {
	case nil:
		// We do not have to read out all entries in read mode.
		// The last record maybe a partial written one, so
		// ErrunexpectedEOF might be returned.
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			state.Reset()
			return nil, state, nil, err
		}
	default:
		// We must read all of the entries if WAL is opened in write mode.
		if err != io.EOF {
			state.Reset()
			return nil, state, nil, err
		}
		// decodeRecord() will return io.EOF if it detects a zero record,
		// but this zero record may be followed by non-zero records from
		// a torn write. Overwriting some of these non-zero records, but
		// not all, will cause CRC errors on WAL open. Since the records
		// were never fully synced to disk in the first place, it's safe
		// to zero them out to avoid any CRC errors from new writes.
		if _, err = w.tail().Seek(w.decoder.lastOffset(), io.SeekStart); err != nil {
			return nil, state, nil, err
		}
		if err = fileutil.ZeroToEnd(w.tail().File); err != nil {
			return nil, state, nil, err
		}
	}

	err = nil
	if !match {
		err = ErrSnapshotNotFound
	}

	// close decoder, disable reading
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}
	w.start = walpb.Snapshot{}

	w.metadata = metadata

	if w.tail() != nil {
		// create encoder (chain crc with the decoder), enable appending
		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}
	w.decoder = nil

	return metadata, state, ents, err
}

// ValidSnapshotEntries returns all the valid snapshot entries in the wal logs in the given directory.
// Snapshot entries are valid if their index is less than or equal to the most recent committed hardstate.
func ValidSnapshotEntries(lg *zap.Logger, walDir string) ([]walpb.Snapshot, error) {
	var snaps []walpb.Snapshot
	var state raftpb.HardState
	var err error

	rec := &walpb.Record{}
	names, err := readWALNames(lg, walDir)
	if err != nil {
		return nil, err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, 0, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			snaps = append(snaps, loadedSnap)
		case stateType:
			state = mustUnmarshalState(rec.Data)
		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		}
	}
	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	// filter out any snaps that are newer than the committed hardstate
	n := 0
	for _, s := range snaps {
		if s.Index <= state.Commit {
			snaps[n] = s
			n++
		}
	}
	snaps = snaps[:n:n]

	return snaps, nil
}

// Verify reads through the given WAL and verifies that it is not corrupted.
// It creates a new decoder to read through the records of the given WAL.
// It does not conflict with any open WAL, but it is recommended not to
// call this function after opening the WAL for writing.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If the loaded snap doesn't match with the expected one, it will
// return error ErrSnapshotMismatch.
func Verify(lg *zap.Logger, walDir string, snap walpb.Snapshot) error {
	var metadata []byte
	var err error
	var match bool

	rec := &walpb.Record{}

	names, nameIndex, err := selectWALFiles(lg, walDir, snap)
	if err != nil {
		return err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, nameIndex, false)
	if err != nil {
		return err
	}

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				return ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			// Current crc of decoder must match the crc of the record.
			// We need not match 0 crc, since the decoder is a new one at this point.
			if crc != 0 && rec.Validate(crc) != nil {
				return ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			if loadedSnap.Index == snap.Index {
				if loadedSnap.Term != snap.Term {
					return ErrSnapshotMismatch
				}
				match = true
			}
		// We ignore all entry and state type records as these
		// are not necessary for validating the WAL contents
		case entryType:
		case stateType:
		default:
			return fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	if closer != nil {
		closer()
	}

	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}

	if !match {
		return ErrSnapshotNotFound
	}

	return nil
}

// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
// 切换 WAL 日志文件
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	// 文件阶段
	off, serr := w.tail().Seek(0, io.SeekCurrent)
	if serr != nil {
		return serr
	}

	if err := w.tail().Truncate(off); err != nil {
		return err
	}

	// 同步更新
	if err := w.sync(); err != nil {
		return err
	}

	// 文件名
	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	// 从 filePipeline 中获取一个预先打开的临时 WAL 文件
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	// 将新文件添加到LockedFile数组
	w.locks = append(w.locks, newTail)
	// 计算当前文件的crc
	prevCrc := w.encoder.crc.Sum32()
	// 创建 encoder，并传入之前文件的crc，这样可以前后校验
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	// 保存 crcType 类型的 Record
	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}

	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}

	if err = w.saveState(&w.state); err != nil {
		return err
	}

	// atomically move temp wal file to wal file
	if err = w.sync(); err != nil {
		return err
	}

	off, err = w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// 重命名
	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	start := time.Now()
	// 同步目录
	if err = fileutil.Fsync(w.dirFile); err != nil {
		return err
	}
	walFsyncSec.Observe(time.Since(start).Seconds())

	// reopen newTail with its new path so calls to Name() match the wal filename format
	// 重新打开并上锁新的文件（重命名之后的文件）
	newTail.Close()

	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return err
	}

	// 重新添加到 Locks 数组（替换之前那个临时的）
	w.locks[len(w.locks)-1] = newTail

	prevCrc = w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	if w.lg != nil {
		w.lg.Info("created a new WAL segment", zap.String("path", fpath))
	} else {
		plog.Infof("segmented wal file %v is created", fpath)
	}
	return nil
}

func (w *WAL) sync() error {
	if w.encoder != nil {
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}
	start := time.Now()
	err := fileutil.Fdatasync(w.tail().File)

	took := time.Since(start)
	if took > warnSyncDuration {
		if w.lg != nil {
			w.lg.Warn(
				"slow fdatasync",
				zap.Duration("took", took),
				zap.Duration("expected-duration", warnSyncDuration),
			)
		} else {
			plog.Warningf("sync duration of %v, expected less than %v", took, warnSyncDuration)
		}
	}
	walFsyncSec.Observe(took.Seconds())

	return err
}

func (w *WAL) Sync() error {
	return w.sync()
}

// ReleaseLockTo releases the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.locks) == 0 {
		return nil
	}

	var smaller int
	found := false
	for i, l := range w.locks {
		_, lockIndex, err := parseWALName(filepath.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	// if no lock index is greater than the release index, we can
	// release lock up to the last one(excluding).
	if !found {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

// Close closes the current WAL file and directory.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			if w.lg != nil {
				w.lg.Warn("failed to close WAL", zap.Error(err))
			} else {
				plog.Errorf("failed to unlock during closing wal: %s", err)
			}
		}
	}

	return w.dirFile.Close()
}

// 保存 entryType 类型 Record
func (w *WAL) saveEntry(e *raftpb.Entry) error {
	// TODO: add MustMarshalTo to reduce one allocation.
	b := pbutil.MustMarshal(e)
	rec := &walpb.Record{Type: entryType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	w.enti = e.Index
	return nil
}

// 保存 stateType 类型 Record
func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	w.state = *s
	b := pbutil.MustMarshal(s)
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

// 批量写入 Entry 日志的方法
func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// short cut, do not call sync
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}

	mustSync := raft.MustSync(st, w.state, len(ents))

	// TODO(xiangli): no more reference operator
	// 批量写入 Entry 日志
	for i := range ents {
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}
	// 写入 Entry 日志后再写入一条 stateType 类型 Record
	if err := w.saveState(&st); err != nil {
		return err
	}

	curOff, err := w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if curOff < SegmentSizeBytes {
		if mustSync {
			return w.sync()
		}
		return nil
	}

	return w.cut()
}

// 保存 snapshotType 类型 Record
func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	b := pbutil.MustMarshal(&e)

	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// update enti only when snapshot is ahead of last index
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync()
}

// 保存 crcType 类型 Record
func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

func (w *WAL) tail() *fileutil.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1]
	}
	return nil
}

func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWALName(filepath.Base(t.Name()))
	if err != nil {
		if w.lg != nil {
			w.lg.Fatal("failed to parse WAL name", zap.String("name", t.Name()), zap.Error(err))
		} else {
			plog.Fatalf("bad wal name %s (%v)", t.Name(), err)
		}
	}
	return seq
}

func closeAll(rcs ...io.ReadCloser) error {
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

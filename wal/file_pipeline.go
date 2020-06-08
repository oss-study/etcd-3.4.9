// Copyright 2016 The etcd Authors
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
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/pkg/fileutil"

	"go.uber.org/zap"
)

// filePipeline pipelines allocating disk space
type filePipeline struct {
	lg *zap.Logger

	// dir to put files
	// 存放临时文件的目录
	dir string
	// size of files to make, in bytes
	// 创建临时文件时预分配空间的大小，默认是 64MB
	size int64
	// count number of files generated
	// 当前 filePipeline 实例创建的临时文件数
	count int

	// 新建的临时文件句柄会通过 filec 返回给 WAL 实例使用 。
	filec chan *fileutil.LockedFile
	// 当创建临时文件出现异常时，则将异常传递到 errc 中。
	errc chan error
	// 当 filePipeline.Close() 方法被调用时会关闭 donec 通道，从而通知 filePipeline 实例删除最后一次创建的临时文件。
	donec chan struct{}
}

func newFilePipeline(lg *zap.Logger, dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{
		lg:    lg,
		dir:   dir,
		size:  fileSize,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	// 启动一个后台 goroutine 创建新的临时文件并将其句柄传递到 filec 中
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	// 从 filec 中获取已经创建好的临时文件
	case f = <-fp.filec:
	// 	如果创建临时文件时出现异常
	case err = <-fp.errc:
	}
	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
	// count % 2 so this file isn't the same as the one last published
	// 为了防止与前一个创建的临时文件重名，新建临时文件的编号是 0 或是 1
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	// 创建临时文件
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return nil, err
	}
	// 尝试预分配空间，如果当前系统不支持预分配文件空间，那么会产生一条警告日志，但不会报错
	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
		if fp.lg != nil {
			fp.lg.Warn("failed to preallocate space when creating a new WAL", zap.Int64("size", fp.size), zap.Error(err))
		} else {
			plog.Errorf("failed to allocate space when creating new wal file (%v)", err)
		}
		f.Close()
		return nil, err
	}
	fp.count++
	return f, nil
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		// 调用 alloc() 方法创建临时文件
		f, err := fp.alloc()
		// 如果创建临时文件失败，则将异常传递到 errc 中
		if err != nil {
			fp.errc <- err
			return
		}
		select {
		// 将上面创建的临时文件句柄传递到 filec 中
		case fp.filec <- f:
		// 调用 filePipeline.Close() 方法时会关闭该通道，删除最后一次创建的临时文件
		case <-fp.donec:
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}

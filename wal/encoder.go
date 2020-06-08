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
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"go.etcd.io/etcd/pkg/crc"
	"go.etcd.io/etcd/pkg/ioutil"
	"go.etcd.io/etcd/wal/walpb"
)

// walPageBytes is the alignment for flushing records to the backing Writer.
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
const walPageBytes = 8 * minSectorSize

type encoder struct {
	mu sync.Mutex
	// PageWriter 是带有缓冲区的 Writer，在写入时，每写满一个 Page 大小的缓冲区，就会自动触发一次 Flush 操作，将数据同步刷新到磁盘上。
	// 每个 Page 的大小是由 walPageBytes 常量指定的，默认是 4KB，即一个操作系统页大小
	bw *ioutil.PageWriter

	crc hash.Hash32
	// 日志序列化之后会暂时存储在该缓冲区中，该缓冲区会被复用，防止了每次序列化创建缓冲区带来的开销
	buf []byte
	// 该缓冲区用来暂存一个 Frame 的长度的数据（Frame 由日志数据和填充数据构成）。
	uint64buf []byte
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc: crc.New(prevCrc, crcTable),
		// 1MB buffer
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// newFileEncoder creates a new encoder with current file offset for the page writer.
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

// 将 Record 序列化后持久化到 WAL 文件
func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.crc.Write(rec.Data)
	// 生成数据的 crc
	rec.Crc = e.crc.Sum32()
	var (
		data []byte
		err  error
		n    int
	)

	if rec.Size() > len(e.buf) {
		// 如果超过预分配的 buf，就使用动态分配
		data, err = rec.Marshal()
		if err != nil {
			return err
		}
	} else {
		// 使用预分配的 buf
		n, err = rec.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}

	// 计算数据填充长度
	lenField, padBytes := encodeFrameSize(len(data))
	// 写入 Record 编码后的长度（占用的字节数）
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}

	// 写入填充数据
	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	// 写入 Record 数据
	n, err = e.bw.Write(data)
	walWriteBytes.Add(float64(n))
	return err
}

// 在 encodeFrameSize() 方法会完成 8 字节对齐，
// 这里会将真正的数据和填充数据看作一个 Frame，返回值是整个 Frame 的长度和填充数据的长度。
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		// 第一位用来标记是否含有填充数据（有为1），随后的七位用来表示填充数据的大小，高 56 位用于表示 Record 的实际大小
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

func (e *encoder) flush() error {
	e.mu.Lock()
	n, err := e.bw.FlushN()
	e.mu.Unlock()
	walWriteBytes.Add(float64(n))
	return err
}

func writeUint64(w io.Writer, n uint64, buf []byte) error {
	// http://golang.org/src/encoding/binary/binary.go
	binary.LittleEndian.PutUint64(buf, n)
	nv, err := w.Write(buf)
	walWriteBytes.Add(float64(nv))
	return err
}

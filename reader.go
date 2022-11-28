// MIT License

// Copyright (c) 2022 liaochuntao

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package filebeat

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
)

var (
	delimLabel byte = '\n'

	// ErrorRename 文件被重命名错误
	ErrorRename error = errors.New("log already rename")

	// ErrorRemoved 文件被移走错误
	ErrorRemoved error = errors.New("log already removed")

	// ErrorEmpty 空数据错误
	ErrorEmpty error = errors.New("no content")

	// ErrorClosed Reader 已经被关闭了
	ErrorClosed error = errors.New("reader already closed")
)

// Reader is the interface that wraps the basic Next method for
// getting a new message.
// Next returns the message being read or and error. EOF is returned
// if reader will not return any new message on subsequent calls.
type Reader interface {
	io.Closer
	// Offset
	//  @return int64
	Offset() int64
	// CurFile
	//  @return *os.File
	CurFile() *os.File
	// Next
	//  @return Message
	//  @return error
	Next() (string, error)
}

// LineReader 按行读取的 line-reader 实现
type LineReader struct {
	closed     int32
	originName string
	curFile    *os.File
	readOffset *int64
	reader     *bufio.Scanner
}

// NewLineReader 构造一个 Reader
func NewLineReader(name string, offset *int64) (Reader, error) {
	f, err := readOpen(name)
	if err != nil {
		return nil, err
	}

	// 设置文件读取的位置信息数据
	f.Seek(func() int64 {
		if *offset == 0 {
			return *offset
		}
		return *offset + 1
	}(), io.SeekStart)

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	return &LineReader{
		originName: name,
		curFile:    f,
		reader:     scanner,
		readOffset: offset,
	}, nil
}

// CurFile
func (line *LineReader) CurFile() *os.File {
	return line.curFile
}

// Offset
func (line *LineReader) Offset() int64 {
	return *line.readOffset
}

// Close
func (line *LineReader) Close() error {
	atomic.StoreInt32(&line.closed, 1)
	line.reader = nil
	return line.curFile.Close()
}

// Next
func (line *LineReader) Next() (string, error) {

	if atomic.LoadInt32(&line.closed) == 1 {
		return "", ErrorClosed
	}

	if line.reader.Scan() {
		msg := line.reader.Text()

		// 需要去掉 '\n'
		// ReadSlice 会把分隔符也一并带上，这里是有问题的，需要单独进行处理把分隔符清理掉
		res := strings.Split(string(msg), string(delimLabel))
		(*line.readOffset) += int64(len(msg))
		return res[0], nil
	}

	err := line.reader.Err()
	if err == nil {
		err = io.EOF
	}
	if errors.Is(err, io.EOF) {
		f, err := readOpen(line.originName)
		if err != nil {
			// 如果当前文件找不到，肯定是文件不一样了
			if errors.Is(err, os.ErrNotExist) {
				return "", ErrorRemoved
			}
			return "", err
		}

		// 当前文件已经被删除
		if isRemoved(line.curFile) {
			return "", ErrorRemoved
		}

		// 已经不是同一个日志文件了，并且当前文件已经读完，准备读取新的日志文件
		if !isSameFile(line.curFile, f) {
			return "", ErrorRename
		}
	}
	return "", err
}

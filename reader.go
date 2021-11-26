//@Author: springliao
//@Description:
//@Time: 2021/11/17 12:22

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
	reader     *bufio.Reader
}

// NewLineReader
//  @param name
//  @param offset
//  @return Reader
//  @return error
func NewLineReader(name string, offset *int64) (Reader, error) {
	f, err := ReadOpen(name)
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

	return &LineReader{
		originName: name,
		curFile:    f,
		reader:     bufio.NewReader(f),
		readOffset: offset,
	}, nil
}

// CurFile
//  @receiver line
//  @return *os.File
func (line *LineReader) CurFile() *os.File {
	return line.curFile
}

// Offset
//  @receiver line
//  @return int64
func (line *LineReader) Offset() int64 {
	return *line.readOffset
}

// Close
//  @receiver line
//  @return error
func (line *LineReader) Close() error {
	atomic.StoreInt32(&line.closed, 1)
	line.reader = nil
	return line.curFile.Close()
}

// Next
//  @receiver line
//  @return string
//  @return error
func (line *LineReader) Next() (string, error) {

	if atomic.LoadInt32(&line.closed) == 1 {
		return "", ErrorClosed
	}

	msg, err := line.reader.ReadSlice(delimLabel)

	if err == nil {
		// 需要去掉 '\n'
		// ReadSlice 会把分隔符也一并带上，这里是有问题的，需要单独进行处理把分隔符清理掉
		res := strings.Split(string(msg), string(delimLabel))
		(*line.readOffset) += int64(len(msg))
		return res[0], nil
	}

	if err != nil {
		if errors.Is(err, io.EOF) {
			f, err := ReadOpen(line.originName)
			if err != nil {
				// 如果当前文件找不到，肯定是文件不一样了
				if errors.Is(err, os.ErrNotExist) {
					return "", ErrorRemoved
				}
				return "", err
			}

			// 当前文件已经被删除
			if IsRemoved(line.curFile) {
				return "", ErrorRemoved
			}

			// 已经不是同一个日志文件了，并且当前文件已经读完，准备读取新的日志文件
			if !IsSameFile(line.curFile, f) {
				return "", ErrorRename
			}
		}
	}
	return "", err
}

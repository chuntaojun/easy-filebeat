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

//@Author: springliao
//@Description:
//@Time: 2021/11/17 15:17

package filebeat

import (
	"os"
	"strconv"
	"syscall"
)

// cpoy from filebeat

type StateOS struct {
	Inode  uint64 `json:"inode," struct:"inode"`
	Device uint64 `json:"device," struct:"device"`
}

// GetOSState returns the FileStateOS for non windows systems
func GetOSState(info os.FileInfo) StateOS {
	stat := info.Sys().(*syscall.Stat_t)

	// Convert inode and dev to uint64 to be cross platform compatible
	fileState := StateOS{
		Inode:  uint64(stat.Ino),
		Device: uint64(stat.Dev),
	}

	return fileState
}

// IsSame file checks if the files are identical
func (fs StateOS) IsSame(state StateOS) bool {
	return fs.Inode == state.Inode && fs.Device == state.Device
}

func (fs StateOS) String() string {
	var buf [64]byte
	current := strconv.AppendUint(buf[:0], fs.Inode, 10)
	current = append(current, '-')
	current = strconv.AppendUint(current, fs.Device, 10)
	return string(current)
}

// ReadOpen opens a file for reading only
func ReadOpen(path string) (*os.File, error) {
	flag := os.O_RDONLY
	perm := os.FileMode(0)
	return os.OpenFile(path, flag, perm)
}

// IsRemoved checks wheter the file held by f is removed.
func IsRemoved(f *os.File) bool {
	stat, err := f.Stat()
	if err != nil {
		// if we got an error from a Stat call just assume we are removed
		return true
	}
	sysStat := stat.Sys().(*syscall.Stat_t)
	return sysStat.Nlink == 0
}

// InodeString returns the inode in string.
func (s *StateOS) InodeString() string {
	return strconv.FormatUint(s.Inode, 10)
}

// IsSameFile 判断是否为同一个文件
func IsSameFile(a, b *os.File) bool {

	as, _ := a.Stat()
	bs, _ := b.Stat()

	return os.SameFile(as, bs)
}

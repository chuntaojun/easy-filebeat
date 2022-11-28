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

package filebeat_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	filebeat "github.com/chuntaojun/easy-filebeat"
)

func Test_ReaderLine(t *testing.T) {

	expectLines := []string{
		"test_line_log=1",
		"test_line_log=2",
		"test_line_log=3",
		"test_line_log=4",
		"test_line_log=5",
		"test_line_log=6",
		"test_line_log=7",
		"test_line_log=8",
		"test_line_log=9",
		"test_line_log=10",
	}

	offset := int64(0)

	reader, err := filebeat.NewLineReader("./test_linereader_file.log", &offset)
	if err != nil {
		t.Fatal(err)
	}

	index := 0
	for {
		msg, err := reader.Next()
		if err != nil {
			if errors.Is(err, filebeat.ErrorRemoved) || errors.Is(err, filebeat.ErrorClosed) || errors.Is(err, io.EOF) {
				t.Log(err)
				return
			}
			t.Fatal(err)
		}

		if strings.Compare(msg, expectLines[index]) != 0 {
			t.Fatalf("no equal, expect=[%s], acutal=[%s]", expectLines[index], msg)
		}

		index++
	}
}

//@Author: chuntaojun <liaochuntao@live.com>
//@Description:
//@Time: 2021/11/23 21:35

package test

import (
	"strings"
	"testing"

	"github.com/chuntaojun/easy-filebeat/filebeat"
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

	msg, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(msg, len(msg))

	if strings.Compare(msg, expectLines[0]) != 0 {
		t.Fatalf("no equal, expect=[%s], acutal=[%s]", expectLines[0], msg)
	}
}

//@Author: springliao
//@Description:
//@Time: 2021/11/17 12:28

package filebeat

// Metadata 记录文件处理信息数据
type Metadata struct {

	// CurFile 正在处理的文件
	CurFile string

	// CurFileINode 当前处理文件的 INode 信息
	CurFileINode string

	// CurOffset 正在处理文件的当前读取的位点信息
	CurOffset int64

	// PreFileINode 上一个被处理完的文件的 INode 信息
	PreFileINode string
}

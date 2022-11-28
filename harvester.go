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
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	EmptyWaitFiles error = errors.New("empty wait files")
)

// Config easy-filebeat 的配置信息
type Config struct {
	// Path 监听的文件路径
	// 支持完全匹配以及正则匹配(监听对应规则的文件)
	Path string
	// MetaPath 元数据保存的位置
	MetaPath string
	// Logger 日志输出
	Logger *logrus.Logger
}

// Harvester 监听文件变动
type Harvester interface {
	io.Closer
	// Init 初始化 Harvester
	Init() error
	// RegisterSink 注册一个处理文件的 Sink 处理者
	RegisterSink(sink Sink)
	// Run 执行监听逻辑
	Run(ctx context.Context)
	// OnError 出现异常时的回掉
	OnError(err error)
}

// NewHarvester 创建一个 Harvester 实例
func NewHarvester(cfg Config) (Harvester, error) {
	beater := &harvester{
		cfg:           cfg,
		meta:          Metadata{},
		waitDealFiles: make([]os.FileInfo, 0),
		logger:        cfg.Logger,
		haveFileCh:    make(chan struct{}, 1),
	}

	beater.haveFileCond = sync.NewCond(&beater.lock)

	if err := beater.Init(); err != nil {
		return nil, err
	}

	return beater, nil
}

// harvester
type harvester struct {
	lock  sync.RWMutex
	sLock sync.RWMutex

	cfg       Config
	curReader atomic.Value
	meta      Metadata
	sinks     []Sink

	waitDealFiles []os.FileInfo

	logger *logrus.Logger

	haveFileCond *sync.Cond
	haveFileCh   chan struct{}
}

// Init
func (beater *harvester) Init() error {
	metaPath := beater.cfg.MetaPath
	// 之前是否存在元数据记录文件
	data, err := ioutil.ReadFile(metaPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := os.Create(metaPath); err != nil {
			return err
		}
	} else {
		if !json.Valid(data) {
			return nil
		}
		// 读取上次工作的元数据文件信息
		if err := json.Unmarshal(data, beater.meta); err != nil {
			return err
		}
	}
	// 根据 metadat 初始化 Reader
	if err := beater.initReaderFromMetadata(); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		// 如果根据 metadata 里面记录的元数据找不到之前处理过的文件，那就认为是重新开始吧
		beater.meta = Metadata{}
		return beater.initReaderFromMetadata()
	}
	return nil
}

// Run 执行监听逻辑
func (beater *harvester) Run(ctx context.Context) {

	// 开启定时刷新待处理文件列表信息
	go func(ctx context.Context) {
		// 先立马刷新一次
		beater.refreshWaitDealFileList()
		ticker := time.NewTicker(time.Duration(5 * time.Second))

		for {
			select {
			case <-ticker.C:
				beater.refreshWaitDealFileList()
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}(ctx)

	//
	go func() {
		// 设置待处理文件列表信息数据
		if err := beater.setWaitDealFiles(); err != nil {
			beater.OnError(err)
		}
	}()

	//
	go func(ctx context.Context) {
		// 元数据没有任何信息
		if beater.curReader.Load() == nil {
			if err := beater.initReaderFromWait(); err != nil {
				beater.OnError(err)
				return
			}
		}

		ticker := time.NewTicker(time.Duration(50 * time.Millisecond))

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				beater.innerRun()
			}
		}
	}(ctx)
}

func (beater *harvester) innerRun() {
	for {
		curReader := beater.curReader.Load().(Reader)
		msg, err := curReader.Next()
		if err != nil {
			switch err {
			case ErrorRemoved, ErrorRename:
				// 切换文件，转到下一个要处理的
				beater.switchNextFile()
			case io.EOF:
				// 当前日志文件还没触发切换，也没有新的数据可供读取，因此进入重试等待
				return
			case os.ErrNotExist:
				// 不存在文件
				fallthrough
			default:
				beater.OnError(err)
				return
			}
		} else {
			beater.sLock.RLock()
			for i := range beater.sinks {
				beater.sinks[i].OnMessage(msg)
			}
			beater.sLock.RUnlock()

			// 上报当前的metadat数据并持久化
			beater.reportAndSyncMetadata()
			continue
		}
	}
}

func (beater *harvester) OnError(err error) {
	beater.logger.Errorf("harvester onError : %s", err.Error())
}

// RegisterSink 注册一个 Sink 用与接收处理 harvester 获取的每行数据
//
//	@receiver beater
//	@param sink
func (beater *harvester) RegisterSink(sink Sink) {
	beater.sLock.Lock()
	defer beater.sLock.Unlock()

	beater.sinks = append(beater.sinks, sink)
}

// Close
//
//	@receiver beater
//	@return error
func (beater *harvester) Close() error {
	return beater.curReader.Load().(Reader).Close()
}

// initReaderFromMetadata
func (beater *harvester) initReaderFromMetadata() error {

	if beater.meta.CurFile != "" {
		curReader, err := NewLineReader(beater.meta.CurFile, &beater.meta.CurOffset)
		if err != nil {
			return err
		}
		beater.curReader.Store(curReader)
	}

	return nil

}

// initReaderFromWait
//
//	@receiver beater
func (beater *harvester) initReaderFromWait() error {

	beater.lock.Lock()
	if len(beater.waitDealFiles) == 0 {
		beater.haveFileCond.Wait()
	}

	waitDeal := beater.waitDealFiles[len(beater.waitDealFiles)-1]

	// 更新 metadata 数据信息
	beater.meta.CurFile = waitDeal.Name()
	beater.meta.CurOffset = 0
	beater.meta.CurFileINode = GetOSState(waitDeal).String()
	beater.lock.Unlock()

	// 构造行读取 Reader
	curReader, err := NewLineReader(waitDeal.Name(), &beater.meta.CurOffset)
	if err != nil {
		return err
	}

	old := beater.curReader.Load()
	if old != nil {
		old.(Reader).Close()
	}

	beater.curReader.Store(curReader)
	return nil
}

// refreshWaitDealFileList 定时刷新当前待处理的文件列表
//
//	@receiver beater
func (beater *harvester) refreshWaitDealFileList() {
	ticker := time.NewTicker(time.Duration(5 * time.Second))

	for range ticker.C {
		if err := beater.setWaitDealFiles(); err != nil {
			continue
		}
	}
}

// setWaitDealFiles 更新待处理的文件列表
//
//	@receiver beater
//	@return error
func (beater *harvester) setWaitDealFiles() error {
	var (
		result []os.FileInfo
		err    error
	)

	for {
		result, err = beater.loadCurFiles()
		if err != nil {
			return err
		}

		// 如果当前获取到的文件列表为空
		result = beater.ignoreAlreadDeal(result)

		if len(result) == 0 {
			beater.logger.Info("cur dir is empty, so wait 200 mill and scan again")
			time.Sleep(time.Duration(200 * time.Millisecond))
		} else {
			break
		}
	}

	// 更新待处理文件列表
	func() {
		beater.lock.Lock()
		defer beater.lock.Unlock()

		beater.waitDealFiles = result
	}()

	// 广播通知有新的文件可以用了
	beater.haveFileCond.Broadcast()

	return nil
}

// switchNextFile
//
//	@receiver beater
//	@return error
func (beater *harvester) switchNextFile() error {
	// 关闭之前的文件 Reader
	old := beater.curReader.Load()
	if old != nil {
		old.(Reader).Close()
	}

	func() {
		beater.lock.Lock()
		defer beater.lock.Unlock()

		// 第一步：根据 INode info 移除指定的 os.FileInfo
		curStatStr := beater.meta.CurFileINode
		pos := -1
		for i := range beater.waitDealFiles {
			if curStatStr == GetOSState(beater.waitDealFiles[i]).String() {
				pos = i
				break
			}
		}

		if pos != -1 {
			beater.waitDealFiles = append(beater.waitDealFiles[:pos], beater.waitDealFiles[pos+1:]...)
		}
	}()

	// 第二步：调用 initReaderFromWait 进行文件的切换动作
	return beater.initReaderFromWait()
}

// ignoreAlreadDeal
//
//	@receiver beater
//	@param source
//	@return []os.FileInfo
func (beater *harvester) ignoreAlreadDeal(source []os.FileInfo) []os.FileInfo {

	// 按照修改时间进行逆序排序
	sort.Slice(source, func(i, j int) bool {
		return source[i].ModTime().After(source[j].ModTime())
	})

	// 这里是逆序的结果

	pos := len(source)
	for i := range source {
		item := source[i]
		curINodeInfo := GetOSState(item).String()

		if beater.meta.CurFileINode == curINodeInfo {
			pos = i
			break
		}
	}

	return source[:pos]
}

// loadCurFiles 获取要监听的日志目录下的所有日志文件信息
//
//	@receiver beater
//	@return []os.FileInfo
//	@return error
func (beater *harvester) loadCurFiles() ([]os.FileInfo, error) {
	parentDir := filepath.Dir(beater.cfg.Path)

	fList, err := ioutil.ReadDir(parentDir)
	if err != nil {
		return nil, err
	}

	// 正则编译，准备用于判断感兴趣的文件列表
	regx, err := regexp.Compile(filepath.Base(beater.cfg.Path))
	if err != nil {
		return nil, err
	}

	target := make([]os.FileInfo, 0)

	for i := range fList {
		item := fList[i]
		if regx.Match([]byte(item.Name())) {
			target = append(target, item)
		}
	}

	return target, nil
}

// reportAndSyncMetadata 上报当前的数据处理情况
func (beater *harvester) reportAndSyncMetadata() {
	// TODO 这里目前是实时落盘，感觉这里可以用 mmap 的方式，加快写的速度，然后将落盘的时机转交操作系统完成
	data, _ := json.Marshal(beater.meta)
	ioutil.WriteFile(beater.cfg.MetaPath, data, fs.ModeAppend)
}

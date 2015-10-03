package gomdic

import (
	"time"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"code.google.com/p/log4go"
)

type MdicFileInfo struct {
	file_path     string
	md5_file_path string
	md5_str       string
}
type MDicParserInf interface {
	ParseLine(line string) (k interface{}, v interface{})
}
type mdic struct {
	parserDic    map[string]MDicParserInf
	fileDic      map[string]*MdicFileInfo
	mutexDic     map[string]*sync.RWMutex
	dataDic      map[string]map[interface{}]interface{}
	panicMaxSIze int
	log4sys      log4go.Logger
	updateInterval time.Duration
}

var singleMdic *mdic = nil

func GetMdic() *mdic {
	if singleMdic == nil {
		panic(errors.New("GetMdic Error"))
	}
	return singleMdic
}
func InitMdic(log log4go.Logger, updateInterval time.Duration) error {
	//TODO 初始化mdic
	singleMdic = &mdic{
		parserDic:    make(map[string]MDicParserInf),
		fileDic:      make(map[string]*MdicFileInfo),
		mutexDic:     make(map[string]*sync.RWMutex),
		dataDic:      make(map[string]map[interface{}]interface{}),
		panicMaxSIze: 10,
		log4sys:      log,
		updateInterval : updateInterval,
	}
	//后台定时更新任务
	go func(){
		for {
			singleMdic.log4sys.Trace("[mdic]后台更新作业开始执行 updateInterval[%d]", singleMdic.updateInterval/time.Second)

			singleMdic.UpdateData()
			time.Sleep(singleMdic.updateInterval)
		}
	}()
	return nil
}

func (m *mdic) Register(name, file_path string, dicPtr MDicParserInf) {

	_, ok := m.fileDic[name]
	if ok {
		panic(errors.New("Mdic multi Register"))
	}
	//TODO 注册文件
	md5_file_path := file_path + ".md5"
	fileInfo:= &MdicFileInfo{
		file_path:     file_path,
		md5_file_path: md5_file_path,
		md5_str:       ""}
	//TODO 注册行parser
	m.parserDic[name] = dicPtr
	//TODO 注册锁
	m.mutexDic[name] = &sync.RWMutex{}
	//TODO 首次注册，需要load当前注册的数据
	m.UpdateDataByNameAndFileInfo(name, fileInfo)
	//TODO 注册fileInfo
	m.fileDic[name] = fileInfo
	
	m.log4sys.Trace("file_path[%s]   md5_file_path[%s]\n", file_path, md5_file_path)
	
}
func (m *mdic) UpdateDataByNameAndFileInfo(name string, fileInfo *MdicFileInfo) {

	m.log4sys.Trace("name[%s], fileInfo[%s]", name, fileInfo)
	f_md5, err := os.Open(fileInfo.md5_file_path)
	if err != nil {
		str := fmt.Sprintf("[mdic] os.Open error fileInfo[%s] error[%s]", fileInfo, err.Error())
		m.log4sys.Error(str)
		return
	}
	buff := bufio.NewReader(f_md5)
	line, err := buff.ReadString('\n')
	if err != nil && err != io.EOF { //读一行md5文件出错
		m.log4sys.Error("读取md5文件出错 line[%s], err[%s]", line, err.Error())
		return
	}
	line_arr := strings.Split(line, " ")
	if len(line_arr) != 2 {
		m.log4sys.Error("md5文件split len 错误。 line=" + line)
		return
	}
	md5_str := line_arr[0]
	if md5_str == fileInfo.md5_str { //文件未更新，不处理
		m.log4sys.Trace("name[%s] md5文件未更新", name)
		return
	}
	f_md5.Close()

	//TODO 文件有更新，开始重新加载字典
	f_data, err := os.Open(fileInfo.file_path)
	if err != nil {
		m.log4sys.Error(err.Error())
		return
	}
	buff = bufio.NewReader(f_data)
	mutex, ok := m.mutexDic[name]
	if !ok {
		m.log4sys.Error("Get mutex error")
		return
	}
	tmpData := make(map[interface{}]interface{})

	read_num, error_num := 0, 0
	lineParser, ok := m.parserDic[name]
	if !ok { //获取当前文件的每行的parser
		str := fmt.Sprint("getLineParser Error fileInfo[%s], error[%s]", fileInfo, err.Error())
		m.log4sys.Error(str)
		return
	}

	for {
		line, err = buff.ReadString('\n')
		read_num += 1
		if err != nil && io.EOF != err { //读入出错，并且不是到行尾
			error_num += 1
			m.log4sys.Error(err.Error() + " line = " + line)
			continue
		}

		//处理一行数据
		k, v := lineParser.ParseLine(line)
		if k == nil || v == nil {
			error_num += 1
			str := fmt.Sprintf("lineParser return nil. key[%s], value[%s]", k, v)
			m.log4sys.Error(str)
		} else {
			tmpData[k] = v //加入数据
		}
		//是最后一行
		if err != nil && io.EOF == err { //已经读到了最后一行
			break
		}
	}

	f_data.Close()
	mutex.Lock()
	m.dataDic[name] = tmpData //写入数据
	mutex.Unlock()
	//TODO 更新最新md5串
	fileInfo.md5_str = md5_str
	
	m.log4sys.Trace("[mdic] 文件路径[%s], 读入行数[%d], 出错行数[%d], 去重后有效行数[%d], md5_str[%s]", fileInfo.file_path, read_num, error_num, len(tmpData), fileInfo.md5_str)

}
func (m *mdic) UpdateData() {
	m.log4sys.Trace("Update Mdic")

	//TODO 遍历fileDic，检查是否有更新
	for name, fileInfo := range m.fileDic {
		m.UpdateDataByNameAndFileInfo(name, fileInfo)
	}
}
func (m *mdic) GetValueByKeys(name string, keys ...interface{}) (values []interface{}, err error) {
	values = make([]interface{}, len(keys))
	err = nil
	mutex := m.mutexDic[name]
	mutex.RLock()
	defer mutex.RUnlock()
	tmpData := m.dataDic[name]
	for idx, key := range keys {
		v, ok := tmpData[key]
		if !ok {
			m.log4sys.Warn("获取value出错 idx[%d], key[%s]", idx, key)
		}
		values[idx] = v
	}
	return
}

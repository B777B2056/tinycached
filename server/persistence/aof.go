package persistence

import (
	"bufio"
	"log"
	"os"
	"sync"
)

type Aof struct {
	buf    []byte
	file   *os.File
	reader *bufio.Reader
}

var aofFilePath = "cache.aof"
var mutex = sync.Mutex{}
var aofInstance *Aof

func checkFileIsExist(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}

func AofInstance() *Aof {
	if aofInstance == nil {
		mutex.Lock()
		if aofInstance == nil {
			aofInstance = newAof()
		}
		mutex.Unlock()
	}
	return aofInstance
}

func newAof() (aof *Aof) {
	var err error
	if checkFileIsExist(aofFilePath) {
		aof.file, err = os.OpenFile(aofFilePath, os.O_APPEND, 0666)
	} else {
		aof.file, err = os.Create(aofFilePath)
	}
	if err != nil {
		panic(err.Error())
	}
	aof.buf = make([]byte, 16)
	aof.reader = bufio.NewReader(aof.file)
	return aof
}

func (aof *Aof) Flush() {
	if len(aof.buf) == 0 {
		return
	}
	n, err := aof.file.Write(aof.buf)
	if err != nil {
		log.Print("AOF write falied")
		return
	}
	aof.buf = aof.buf[n:]
}

func (aof *Aof) Append(cmd []byte, body []byte) {
	aof.buf = append(aof.buf, cmd...)
	aof.buf = append(aof.buf, []byte(" ")...)
	aof.buf = append(aof.buf, body...)
	aof.buf = append(aof.buf, []byte("\n")...)
}

func (aof *Aof) GetOneChar() (byte, bool) {
	line, err := aof.reader.ReadByte()
	if err != nil {
		return '-', false
	}
	return line, true
}

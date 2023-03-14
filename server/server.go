package main

import (
	"io"
	"log"
	"net"
	"strconv"
	"tinycached/server/command"
	"tinycached/server/persistence"
	"tinycached/server/timer"
	"tinycached/utils"
)

/*
 * 基础命令
 * GET KEY名字\n				执行完成后，若找到了则返回值，否则返回NIL\n
 * SET KEY名字 VALUE值\n		执行完成后返回DONE\n
 * DEL KEY名字\n				执行完成后返回DONE\n
 * EXPR KEY名字 过期时间ms值\n	 执行完成后返回DONE\n
 * ----------------------------------------------------------------------------------------------
 * 事务命令
 * MULTI\n				标记事务开始
 * EXEC\n				标记事务执行
 * DISCARD\n			标记事务取消
 * WATCH KEY名字\n		标记该缓存值需要被监视；若其他客户端在事务执行中修改了该缓存值，则事务执行失败，返回NIL\n
 * UNWATCH KEY名字\n	取消监视
 * ----------------------------------------------------------------------------------------------
 */

type CacheServer struct {
	t    *timer.Timer
	port uint16
}

func NewServer(port uint16) (svr *CacheServer) {
	svr = &CacheServer{
		t:    timer.NewTimer(),
		port: port,
	}
	svr.init()
	return svr
}

func (svr *CacheServer) init() {
	aof := persistence.AofInstance()
	// 逐条执行命令，恢复缓存状态
	isStop := false
	clt := command.NewCacheClient()
	for !isStop {
		// 每次接受一个字符，进入协议解析状态机，并调用相关命令的api
		cmd, body, ok := utils.ParseFSM(func() (byte, bool) {
			ch, ok := aof.GetOneChar()
			if !ok {
				isStop = true
				return '-', false
			}
			return ch, true
		})
		if ok {
			_, err := clt.ExecCmd(cmd, body)
			if err != nil {
				break
			}
		} else {
			break
		}
	}
	// 定时检查AOF并刷入硬盘
	svr.t.Start(func() { aof.Flush() })
}

func (svr *CacheServer) run() {
	defer svr.t.Stop()
	listen, err := net.Listen("tcp", ":"+strconv.FormatUint(uint64(svr.port), 10))
	if err != nil {
		panic(err)
	}
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go svr.handler(conn, command.NewCacheClient())
	}
}

func (svr *CacheServer) handler(conn net.Conn, clt *command.CacheClientInfo) {
	isStop := false
	char := make([]byte, 1)
	for !isStop {
		// 每次接受一个字符，进入协议解析状态机，并调用相关命令的api
		var result []byte
		cmd, body, ok := utils.ParseFSM(func() (byte, bool) {
			if _, err := conn.Read(char); err != nil {
				if err == io.EOF {
					isStop = true
				}
				return '-', false
			}
			return char[0], true
		})
		if ok {
			ret, err := clt.ExecCmd(cmd, body)
			if err != nil {
				result = []byte(err.Error())
			} else {
				result = ret
			}
		} else {
			result = []byte("FAILED")
		}
		utils.WriteAll(conn, result)
	}
}

func main() {
	svr := NewServer(7000)
	svr.run()
}

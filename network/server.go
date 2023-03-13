package network

import (
	"io"
	"log"
	"net"
	"strconv"
	"tinycached/command"
	"tinycached/persistence"
	"tinycached/timer"
	"tinycached/utils"
)

/*
 * 基础命令
 * SELECT CacheGroup名字\n		选择组，若没有则创建
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
 */

type CacheServer struct {
	t *timer.Timer
}

func (svr *CacheServer) init() {
	// 新建AOF对象
	aof := persistence.AofInstance()
	// 新建Timer对象
	svr.t = timer.NewTimer()
	// 逐条执行命令，恢复缓存状态
	isStop := false
	clt := command.NewCacheClient()
	for !isStop {
		// 每次接受一个字符，进入协议解析状态机，并调用相关命令的api
		svr.parseFSM(clt, func() (byte, bool) {
			ch, ok := aof.GetOneChar()
			if !ok {
				isStop = true
				return '-', false
			}
			return ch, true
		})
	}
	// 定时检查AOF并刷入硬盘
	svr.t.Start(func() { aof.Flush() })
}

func (svr *CacheServer) Run(port uint16) {
	svr.init()
	defer svr.t.Stop()
	listen, err := net.Listen("tcp", ":"+strconv.FormatUint(uint64(port), 10))
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
		result := svr.parseFSM(clt, func() (byte, bool) {
			_, err := conn.Read(char)
			if err != nil {
				if err == io.EOF {
					isStop = true
				}
				return '-', false
			}
			return char[0], true
		})
		conn.Write(result)
	}
}

func (svr *CacheServer) parseFSM(clt *command.CacheClientInfo, recv func() (byte, bool)) []byte {
	cmd := make([]byte, 2)
	body := make([]byte, 8)
	curState := utils.CmdSTATE
	for {
		ch, ok := recv()
		if !ok {
			break
		}

		if curState == utils.CmdSTATE {
			cmd = append(cmd, ch)
			if ch == ' ' {
				curState++
			}
		} else if curState == utils.BodySTATE {
			body = append(body, ch)
			if ch == '\n' {
				curState++
			}
		} else if curState == utils.EndSTATE {
			ret, err := clt.ExecCmd(cmd, body)
			if err != nil {
				return []byte(err.Error())
			} else {
				return ret
			}
		} else {
			break
		}
	}
	return []byte("")
}

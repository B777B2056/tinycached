package main

import (
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"tinycached/server/command"
	"tinycached/server/persistence"
	"tinycached/utils"
)

/*
 * 基础命令
 * GET KEY名字\n				执行完成后，若找到了则返回值，否则返回NIL\n
 * SET KEY名字:VALUE值\n		执行完成后返回DONE\n
 * DEL KEY名字\n				执行完成后返回DONE\n
 * EXPR KEY名字:过期时间毫秒值\n	 执行完成后返回DONE\n
 * ----------------------------------------------------------------------------------------------
 * 事务命令
 * MULTI\n				标记事务开始
 * EXEC\n				标记事务执行
 * DISCARD\n			标记事务取消
 * WATCH KEY名字\n		标记该缓存值需要被监视；若其他客户端在事务执行中修改了该缓存值，则事务执行失败，返回NIL\n
 * UNWATCH KEY名字\n	取消监视
 * ----------------------------------------------------------------------------------------------
 */

func main() {
	svr := newServer(7000)
	svr.run()
}

type CacheServer struct {
	t          *utils.Timer
	port       uint16
	sigChannel chan os.Signal
	isStop     int32
}

func newServer(port uint16) (svr *CacheServer) {
	svr = &CacheServer{
		t:          utils.NewTimer(1), // 1s刷一次磁盘
		port:       port,
		sigChannel: make(chan os.Signal, 1),
		isStop:     0,
	}
	// 恢复历史数据
	svr.recoverHistoryCache()
	// 定时检查AOF并刷入硬盘
	svr.t.Start(func() bool {
		persistence.AofInstance().Flush()
		return atomic.LoadInt32(&svr.isStop) == 0
	})
	// 捕获信号
	signal.Notify(svr.sigChannel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go svr.sigHandler()
	return svr
}

func (svr *CacheServer) recoverHistoryCache() {
	aof := persistence.AofInstance()
	// 逐条执行命令，恢复缓存状态
	clt := command.NewCacheClient()
	for isStop := false; !isStop; {
		// 每次接受一个字符，进入协议解析状态机，并调用相关命令的api
		cmd, args := utils.ParseFsm(func() (byte, bool) {
			ch, ok := aof.GetOneChar()
			if !ok {
				isStop = true
				return '-', false
			}
			return ch, true
		})
		for _, arg := range args {
			if _, err := clt.ExecCmd(cmd, arg); err != nil {
				isStop = true
				break
			}
		}
	}
}

func (svr *CacheServer) run() {
	listen, err := net.Listen("tcp", ":"+strconv.FormatUint(uint64(svr.port), 10))
	if err != nil {
		panic(err)
	}

	for atomic.LoadInt32(&svr.isStop) == 0 {
		conn, err := listen.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go svr.reqHandler(conn, command.NewCacheClient())
	}

	svr.t.Stop()
}

func (svr *CacheServer) sigHandler() {
	<-svr.sigChannel
	atomic.StoreInt32(&svr.isStop, 1)
	log.Print(atomic.LoadInt32(&svr.isStop))
}

func (svr *CacheServer) reqHandler(conn net.Conn, clt *command.CacheClientInfo) {
	char := make([]byte, 1)
	for isFinish := false; !isFinish; {
		if atomic.LoadInt32(&svr.isStop) == 1 {
			break
		}
		// 每次接受一个字符，进入协议解析状态机，并调用相关命令的api
		cmd, args := utils.ParseFsm(func() (byte, bool) {
			if _, err := conn.Read(char); err != nil {
				if err == io.EOF {
					isFinish = true
				}
				return '-', false
			}
			return char[0], true
		})

		for _, arg := range args {
			if ret, err := clt.ExecCmd(cmd, arg); err != nil {
				utils.WriteAll(conn, []byte(err.Error()))
			} else {
				utils.WriteAll(conn, ret)
			}
		}
	}
}

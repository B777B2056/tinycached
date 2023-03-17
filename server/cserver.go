package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
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

var (
	ctx    context.Context
	cancel context.CancelFunc
)

func init() {
	ctx, cancel = context.WithCancel(context.Background())
}

type CacheServer struct {
	sigChan  chan os.Signal
	ticker   *time.Ticker
	listener net.Listener
	wg       sync.WaitGroup
}

func newServer(port uint16) (svr *CacheServer) {
	svr = &CacheServer{
		sigChan: make(chan os.Signal, 1),
		ticker:  time.NewTicker(time.Duration(1) * time.Second), // 1s刷一次磁盘
	}
	// 恢复历史数据
	svr.recoverHistoryCache()
	// 启动AOF定时刷新
	svr.startAof()
	// 捕获信号
	svr.capSignal()
	// 开始监听
	svr.startListen(port)
	return svr
}

func (svr *CacheServer) recoverHistoryCache() {
	aof := persistence.AofInstance()
	// 逐条执行命令，恢复缓存状态
	clt := command.NewCacheClient()
	for {
		// 每次接受一个字符，进入协议解析状态机，并调用相关命令的api
		cmd, args, ok := utils.ParseFsm(func() (byte, bool) {
			return aof.GetOneChar()
		})

		if !ok {
			break
		}

		for _, arg := range args {
			if _, err := clt.ExecCmd(cmd, arg); err != nil {
				return
			}
		}
	}
}

func (svr *CacheServer) startAof() {
	go func() {
		for {
			select {
			case <-ctx.Done():
				svr.ticker.Stop()
				return
			case <-svr.ticker.C:
				persistence.AofInstance().Flush()
			}
		}
	}()
}

func (svr *CacheServer) capSignal() {
	signal.Notify(svr.sigChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-svr.sigChan
		svr.listener.Close()
		cancel()
	}()
}

func (svr *CacheServer) startListen(port uint16) {
	var err error
	svr.listener, err = net.Listen("tcp", ":"+strconv.FormatUint(uint64(port), 10))
	if err != nil {
		panic(err)
	}
}

func (svr *CacheServer) run() {
	for {
		select {
		case <-ctx.Done():
			svr.wg.Wait()
			return
		default:
			conn, err := svr.listener.Accept()
			if err != nil {
				continue
			}
			svr.wg.Add(1)
			go svr.reqHandler(conn, command.NewCacheClient())
		}
	}
}

func (svr *CacheServer) reqHandler(conn net.Conn, clt *command.CacheClientInfo) {
	char := make([]byte, 1)
	for {
		select {
		case <-ctx.Done():
			log.Print("reqHandler")
			return
		default:
			// 每次接受一个字符，进入协议解析状态机，并调用相关命令的api
			cmd, args, ok := utils.ParseFsm(func() (byte, bool) {
				_, err := conn.Read(char)
				return char[0], (err == nil)
			})

			if !ok {
				return
			}

			for _, arg := range args {
				if ret, err := clt.ExecCmd(cmd, arg); err != nil {
					utils.WriteAll(conn, []byte(err.Error()))
				} else {
					utils.WriteAll(conn, ret)
				}
			}
		}
	}
}

func main() {
	svr := newServer(7000)
	svr.run()
}

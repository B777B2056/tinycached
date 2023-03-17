package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"tinycached/proxy/consistenthash"
	"tinycached/utils"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
)

func init() {
	ctx, cancel = context.WithCancel(context.Background())
}

type cacheProxy struct {
	mutex    sync.Mutex
	servers  map[string]net.Conn // 已上线服务器map，key=服务器地址，value=与服务器的连接对象
	clients  map[net.Conn]string // 已连接客户端map，key=与客户端的连接对象，value=上一次客户端发来的key，用于无key命令的服务器定位
	hashmap  *consistenthash.Map // 一致性哈希
	listener net.Listener
	sigChan  chan os.Signal
	wg       sync.WaitGroup
}

func newCacheProxy(port uint16) *cacheProxy {
	proxy := &cacheProxy{
		servers: make(map[string]net.Conn),
		hashmap: consistenthash.NewConsistentHash(3, nil),
		sigChan: make(chan os.Signal, 1),
	}
	// 从json里load服务器地址
	svrsTest := proxy.loadServerAddrs()
	// 与各个服务器建立连接
	for _, svrName := range svrsTest {
		if err := proxy.connectToServer(svrName); err != nil {
			log.Printf("svr %s connect failed", svrName)
			break
		}
	}
	// 捕获信号
	proxy.capSignal()
	// 监听端口
	proxy.startListen(port)
	return proxy
}

// TODO
func (proxy *cacheProxy) loadServerAddrs() []string {
	return []string{"127.0.0.1:7000"}
}

func (proxy *cacheProxy) connectToServer(svrName string) error {
	conn, err := net.Dial("tcp", svrName)
	if err != nil {
		return err
	}
	proxy.servers[svrName] = conn
	proxy.hashmap.AddNode(svrName)
	return nil
}

func (proxy *cacheProxy) capSignal() {
	signal.Notify(proxy.sigChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-proxy.sigChan
		proxy.listener.Close()
		cancel()
	}()
}

func (proxy *cacheProxy) startListen(port uint16) {
	var err error
	proxy.listener, err = net.Listen("tcp", ":"+strconv.FormatUint(uint64(port), 10))
	if err != nil {
		panic(err)
	}
}

func (proxy *cacheProxy) chooseServer(cltConn net.Conn, cmd utils.CmdType, arg string) (string, net.Conn, bool) {
	proxy.mutex.Lock()
	if newKey := getKeyFromCmd(cmd, arg); newKey != "" {
		proxy.clients[cltConn] = newKey
	}
	svrName := proxy.hashmap.FindNode(proxy.clients[cltConn])
	svrConn, ok := proxy.servers[svrName]
	proxy.mutex.Unlock()
	return svrName, svrConn, ok
}

func (proxy *cacheProxy) schedule(cltConn net.Conn) {
	// 接受客户端命令
	char := make([]byte, 1)
	cmd, args, ok := utils.ParseFsm(func() (byte, bool) {
		if _, err := cltConn.Read(char); err != nil {
			return '-', false
		}
		return char[0], true
	})

	if !ok {
		return
	}

	if cmd == utils.ERROR {
		utils.WriteAll(cltConn, []byte("Wrong fomat"))
		return
	}
	// 转发客户端命令
	for _, arg := range args {
		// 根据客户端命令中的key选择对应的服务器
		svrName, svrConn, ok := proxy.chooseServer(cltConn, cmd, arg)
		if !ok {
			utils.WriteAll(cltConn, []byte("EMPTY KEY: Cannot find server"))
			return
		}
		// 将客户端命令发给服务器，并等待服务器回复
		utils.WriteAll(svrConn, []byte(cmd.String()+" "+arg))
		// 将服务器回复转发给客户端
		proxy.waitAndForwardMsg(svrName, svrConn, cltConn)
	}
}

func (proxy *cacheProxy) waitAndForwardMsg(svrName string, svrConn net.Conn, cltConn net.Conn) {
	for char := make([]byte, 1); char[0] != '\n'; {
		if _, err := svrConn.Read(char); err != nil {
			if err == io.EOF {
				// 对端服务器掉线
				proxy.hashmap.RemoveNode(svrName)
				delete(proxy.servers, svrName)
				utils.WriteAll(cltConn, []byte("Server cannot reach"))
			}
			break
		}

		if err := utils.WriteAll(cltConn, char); err != nil {
			break
		}
	}
}

func (proxy *cacheProxy) run() {
	for {
		select {
		case <-ctx.Done():
			proxy.wg.Wait()
			return
		default:
			cltConn, err := proxy.listener.Accept()
			if err != nil {
				continue
			}
			proxy.wg.Add(1)
			go func() {
				for {
					proxy.schedule(cltConn)
				}
			}()
		}
	}
}

func getKeyFromCmd(cmd utils.CmdType, arg string) string {
	if (cmd != utils.MULTI) && (cmd != utils.EXEC) && (cmd != utils.DISCARD) {
		return strings.Split(arg, ":")[0]
	}
	return ""
}

func main() {
	proxy := newCacheProxy(8888)
	proxy.run()
}

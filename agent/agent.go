package main

import (
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"tinycached/agent/consistenthash"
	"tinycached/utils"
)

type cacheAgent struct {
	mutex   sync.Mutex
	servers map[string]net.Conn // 已上线服务器map，key=服务器地址，value=与服务器的连接对象
	clients map[net.Conn]string // 已连接客户端map，key=与客户端的连接对象，value=上一次客户端发来的key，用于无key命令的服务器定位
	hashmap *consistenthash.Map // 一致性哈希
}

func newAgent() *cacheAgent {
	agent := &cacheAgent{
		servers: make(map[string]net.Conn),
		hashmap: consistenthash.NewConsistentHash(3, nil),
	}
	// 从json里load服务器地址，并与各个服务器建立连接
	svrsTest := []string{"127.0.0.1:7000"}
	for _, svrName := range svrsTest {
		conn, err := net.Dial("tcp", svrName)
		if err != nil {
			log.Print(err)
			continue
		}
		agent.servers[svrName] = conn
		agent.hashmap.AddNode(svrName)
	}
	return agent
}

func (agent *cacheAgent) chooseServer(cltConn net.Conn, cmd []byte, body []byte) (net.Conn, bool) {
	agent.mutex.Lock()
	if newKey := getKeyFromCmd(cmd, body); newKey != "" {
		agent.clients[cltConn] = newKey
	}
	svrConn, ok := agent.servers[agent.hashmap.FindNode(agent.clients[cltConn])]
	agent.mutex.Unlock()
	return svrConn, ok
}

func (agent *cacheAgent) schedule(cltConn net.Conn) {
	char := make([]byte, 1)
	for {
		// 接受客户端命令
		cmd, body, ok := utils.ParseFSM(func() (byte, bool) {
			if _, err := cltConn.Read(char); err != nil {
				return '-', false
			}
			return char[0], true
		})
		if !ok {
			continue
		}
		// 根据客户端命令中的key选择对应的服务器
		svrConn, ok := agent.chooseServer(cltConn, cmd, body)
		if !ok {
			utils.WriteAll(cltConn, []byte("EMPTY KEY: Cannot find server"))
			continue
		}
		// 将客户端命令发给服务器，并等待服务器回复
		utils.WriteAll(svrConn, cmd)
		utils.WriteAll(svrConn, []byte(" "))
		utils.WriteAll(svrConn, body)
		// 将服务器回复转发给客户端
		for {
			if _, err := svrConn.Read(char); err != nil {
				if err == io.EOF {
					// 对端服务器掉线
					utils.WriteAll(cltConn, []byte("Server cannot reach"))
					return
				}
				break
			}

			if err := utils.WriteAll(cltConn, char); err != nil {
				if err == io.EOF {
					// 客户端掉线
					return
				}
				break
			}
			if char[0] == '\n' {
				break
			}
		}
	}
}

func (agent *cacheAgent) run(port uint16) {
	var isStop int32 = 0
	sigChannel := make(chan os.Signal, 1)
	go func() {
		<-sigChannel
		atomic.StoreInt32(&isStop, 1)
	}()

	listen, err := net.Listen("tcp", ":"+strconv.FormatUint(uint64(port), 10))
	if err != nil {
		panic(err)
	}

	for atomic.LoadInt32(&isStop) == 0 {
		cltConn, err := listen.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go agent.schedule(cltConn)
	}
}

func getKeyFromCmd(cmd []byte, body []byte) string {
	if (string(cmd) != utils.MULTI.String()) && (string(cmd) != utils.EXEC.String()) && (string(cmd) != utils.DISCARD.String()) {
		return strings.Split(string(body), " ")[0]
	}
	return ""
}

func main() {
	agent := newAgent()
	agent.run(8888)
}

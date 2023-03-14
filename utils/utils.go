package utils

import (
	"net"
)

type PolicyType int16

const (
	PolicyLRU PolicyType = iota
	PolicyLFU
)

type CmdParseFSMState int8

const (
	CmdSTATE CmdParseFSMState = iota
	BodySTATE
	EndSTATE
)

func ParseFSM(recv func() (byte, bool)) (cmd []byte, body []byte, ok bool) {
	cmd = make([]byte, 2)
	body = make([]byte, 8)
	curState := CmdSTATE
	for {
		ch, ok := recv()
		if !ok {
			break
		}

		if curState == CmdSTATE {
			cmd = append(cmd, ch)
			if ch == ' ' {
				curState++
			}
		} else if curState == BodySTATE {
			body = append(body, ch)
			if ch == '\n' {
				curState++
			}
		} else if curState == EndSTATE {
			return cmd, body, true
		} else {
			break
		}
	}
	return nil, nil, false
}

type CmdType int16

const (
	GET CmdType = iota
	SET
	DEL
	EXPR
	MULTI
	EXEC
	DISCARD
	WATCH
	UNWATCH
)

func (cmd CmdType) String() string {
	switch cmd {
	case GET:
		return "GET"
	case SET:
		return "SET"
	case DEL:
		return "DEL"
	case EXPR:
		return "EXPR"
	case MULTI:
		return "MULTI"
	case EXEC:
		return "EXEC"
	case DISCARD:
		return "DISCARD"
	case WATCH:
		return "WATCH"
	case UNWATCH:
		return "UNWATCH"
	default:
		return ""
	}
}

func CopyBytes(src []byte) (dest []byte) {
	dest = make([]byte, len(src))
	copy(dest, src)
	return dest
}

func GetLocalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "0.0.0.0"
}

func WriteAll(conn net.Conn, msg []byte) error {
	sentBytes := 0
	totalBytes := len(msg)
	for sentBytes < totalBytes {
		n, err := conn.Write(msg[sentBytes:])
		if err != nil {
			return err
		}
		sentBytes += n
	}
	return nil
}

package utils

import (
	"net"
)

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
	ERROR
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

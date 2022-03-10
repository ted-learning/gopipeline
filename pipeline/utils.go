package pipeline

import (
	"bufio"
	"net"
)

func flushWriter(newWriter *bufio.Writer) {
	err := newWriter.Flush()
	if err != nil {
		panic(err)
	}
}

func closeListener(listen net.Listener) {
	err := listen.Close()
	if err != nil {
		panic(err)
	}
}

func closeConn(conn net.Conn) {
	err := conn.Close()
	if err != nil {
		panic(err)
	}
}

package pipeline

import (
	"bufio"
	"encoding/gob"
	"io"
	"net"
	"os"
)

type FileJob struct {
	Filename  string
	Offset    int64
	ChunkSize int
	NextAddr  string
}

// NetworkWriteJobSink send job to worker
func NetworkWriteJobSink(addr string, job *FileJob) {
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	go func() {
		defer closeListener(listen)
		conn, err := listen.Accept()
		if err != nil {
			panic(err)
		}
		defer closeConn(conn)

		writeFileJob(conn, job)
	}()
}

// NetworkReadJobAndWriteSortSink read job and execute, send the result to next remote server
func NetworkReadJobAndWriteSortSink(addr string) {
	go func() {
		dial, err := net.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		defer closeConn(dial)

		source := readFileJob(dial)
		for job := range source {
			open, err := os.Open(job.Filename)
			if err != nil {
				panic(err)
			}

			_, err = open.Seek(job.Offset, 0)
			if err != nil {
				panic(err)
			}

			sort := InMemSort(ReadSource(open, job.ChunkSize))
			networkWriteSortDataSink(job.NextAddr, sort)
		}
	}()
}

// NetworkReadSortResultSource read sort result
func NetworkReadSortResultSource(addr string) <-chan int {
	out := make(chan int, 2048)
	go func() {
		for {
			dial, err := net.Dial("tcp", addr)
			if err != nil {
				continue
			}

			source := ReadSource(dial, -1)

			for v := range source {
				out <- v
			}
			break
		}
		close(out)

	}()
	return out
}

// networkWriteSortDataSink send the result to next remote server
func networkWriteSortDataSink(addr string, in <-chan int) {
	go func() {
		listen, err := net.Listen("tcp", addr)
		if err != nil {
			panic(err)
		}
		defer closeListener(listen)
		conn, err := listen.Accept()
		if err != nil {
			panic(err)
		}
		defer closeConn(conn)

		WriteSink(conn, in)
	}()
}

// readFileJob read job from reader
func readFileJob(reader io.Reader) <-chan *FileJob {
	out := make(chan *FileJob)
	go func() {
		newReader := bufio.NewReader(reader)
		for {
			job := FileJob{}
			decoder := gob.NewDecoder(newReader)
			err := decoder.Decode(&job)
			if err != nil {
				break
			}
			out <- &job
		}
		close(out)
	}()
	return out
}

// writeFileJob write job to writer
func writeFileJob(writer io.Writer, job *FileJob) {
	newWriter := bufio.NewWriter(writer)
	encoder := gob.NewEncoder(newWriter)
	err := encoder.Encode(job)
	if err != nil {
		panic(err)
	}

	defer flushWriter(newWriter)
}

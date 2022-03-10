package pipeline

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"time"
)

var start time.Time

func Init() {
	start = time.Now()
}
func ArraySource(source []int) <-chan int {
	out := make(chan int, 2048)
	go func() {
		for _, v := range source {
			out <- v
		}
		close(out)
	}()
	return out
}

func InMemSort(in <-chan int) <-chan int {
	out := make(chan int, 2048)
	go func() {
		var temp []int
		for v := range in {
			temp = append(temp, v)
		}
		fmt.Println("read done:", time.Now().Sub(start))

		sort.Ints(temp)
		fmt.Println("sort done:", time.Now().Sub(start))

		for _, v := range temp {
			out <- v
		}
		close(out)
	}()
	return out
}
func Merge(c1, c2 <-chan int) <-chan int {
	out := make(chan int, 2048)
	go func() {
		v1, ok1 := <-c1
		v2, ok2 := <-c2
		for ok1 || ok2 {
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-c1
			} else {
				out <- v2
				v2, ok2 = <-c2
			}
		}
		close(out)
		fmt.Println("merge done:", time.Now().Sub(start))
	}()
	return out
}

func MergeN(c ...<-chan int) <-chan int {
	mid := len(c) / 2
	if mid == 0 {
		return c[0]
	} else {
		return Merge(MergeN(c[:mid]...), MergeN(c[mid:]...))
	}
}

func ReadSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 2048)
	go func() {
		newReader := bufio.NewReader(reader)
		readSize := 0
		for {
			buffer := make([]byte, 8)
			read, err := newReader.Read(buffer)
			readSize += read
			if read > 0 {
				value := int(binary.BigEndian.Uint64(buffer))
				out <- value
			} else {
				break
			}

			if err != nil || (chunkSize != -1 && readSize >= chunkSize) {
				break
			}
		}
		close(out)
	}()
	return out
}

func WriteSink(writer io.Writer, in <-chan int) {
	newWriter := bufio.NewWriter(writer)
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		_, err := newWriter.Write(buffer)
		if err != nil {
			panic(err)
		}
	}

	defer func(newWriter *bufio.Writer) {
		err := newWriter.Flush()
		if err != nil {
			panic(err)
		}
	}(newWriter)
}

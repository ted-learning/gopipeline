package main

import (
	"fmt"
	"gopipeline/pipeline"
	"os"
	"strconv"
	"time"
)

func closeFile(sourceIn *os.File) {
	err := sourceIn.Close()
	if err != nil {

	}
}

func main() {
	c := createNetworkPipeline("data/source.in", 6)
	writeToFile("data/source.out", c)
	printFile("data/source.out")
}

func createNetworkPipeline(filename string, partitions int) <-chan int {
	open, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer closeFile(open)

	stat, err := open.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := stat.Size()
	chunkSize := fileSize / int64(partitions)

	var sortAddrs []string
	var mergeAddrs []string
	pipeline.Init()
	for i := 0; i < partitions; i++ {
		offset := int64(i) * chunkSize
		if i == partitions-1 {
			chunkSize = -1
		}
		addr := ":" + strconv.Itoa(7000+i)
		fmt.Printf("Send job to %s\n", addr)
		pipeline.NetworkWriteJobSink(addr, &pipeline.FileJob{
			Filename:  filename,
			Offset:    offset,
			ChunkSize: int(chunkSize),
			NextAddr:  addr,
		})
		sortAddrs = append(sortAddrs, addr)
		mergeAddrs = append(mergeAddrs, addr)
	}

	for _, addr := range sortAddrs {
		fmt.Printf("Receive job from %s\n", addr)
		pipeline.NetworkReadJobAndWriteSortSink(addr)
	}

	time.Sleep(5 * time.Second)
	var sorts []<-chan int
	for _, addr := range mergeAddrs {
		fmt.Printf("Receive sort results from %s\n", addr)
		sorts = append(sorts, pipeline.NetworkReadSortResultSource(addr))
	}

	return pipeline.MergeN(sorts...)
}

func printFile(filename string) {
	open, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	readSource := pipeline.ReadSource(open, -1)
	defer closeFile(open)

	max := 0
	for v := range readSource {
		fmt.Println(v)
		max++
		if max >= 1000 {
			break
		}
	}
}

func writeToFile(filename string, c <-chan int) {
	create, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer closeFile(create)

	pipeline.WriteSink(create, c)
}

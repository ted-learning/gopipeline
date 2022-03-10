package main

import (
	"fmt"
	"gopipeline/pipeline"
	"os"
)

func closeFile(sourceIn *os.File) {
	err := sourceIn.Close()
	if err != nil {

	}
}

func main() {
	c := createPipeline("data/source.in", 6)
	writeToFile("data/source.out", c)
	printFile("data/source.out")
}

func createPipeline(filename string, partitions int) <-chan int {
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

	pipeline.Init()
	var sorts []<-chan int
	for i := 0; i < partitions; i++ {
		open, err = os.Open(filename)
		if err != nil {
			panic(err)
		}

		_, err = open.Seek(int64(i)*chunkSize, 0)
		if err != nil {
			return nil
		}

		source := pipeline.ReadSource(open, int(chunkSize))
		sorts = append(sorts, pipeline.InMemSort(source))
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

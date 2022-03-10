package main

import (
	"fmt"
	"gopipeline/pipeline"
	"os"
)

const filename = "data/source.in"
const count = 5000000

func closeFile(sourceIn *os.File) {
	err := sourceIn.Close()
	if err != nil {

	}
}

func main() {
	source := pipeline.RandomSource(count)

	sourceIn, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer closeFile(sourceIn)

	pipeline.WriteSink(sourceIn, source)

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

package main

import (
	"fmt"
	"gopipeline/pipeline"
)

func main() {
	merge := pipeline.Merge(
		pipeline.InMemSort(
			pipeline.ArraySource([]int{2, 5, 3, 9, 1, 30, 12, 3, 6, 7, 0, -1}),
		),
		pipeline.InMemSort(
			pipeline.ArraySource([]int{9, 12, 4, 5456, 22, 19, 21, 32, 61}),
		),
	)

	for v := range merge {
		fmt.Println(v)
	}
}

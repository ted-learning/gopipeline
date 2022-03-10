package main

import (
	"fmt"
	"sort"
)

func main() {
	a := []int{3, 6, 7, 12, 21, 6, 1, 36}
	sort.Ints(a)

	for _, v := range a {
		fmt.Println(v)
	}
}

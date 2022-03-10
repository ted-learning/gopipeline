package pipeline

import "math/rand"

func RandomSource(size int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < size; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

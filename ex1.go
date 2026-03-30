package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
	wg := &sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(goroutineNumber int) {
			defer wg.Done()
			wait := rand.Intn(5)
			time.Sleep(time.Duration(wait) * time.Second)
			fmt.Printf("Горутина со случайной (%d c) задержкой: %d\n", wait, goroutineNumber)
		}(i)
	}

	wg.Wait()
}

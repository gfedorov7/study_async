package main

import (
	"fmt"
	"sync"
)

const (
	CountGoroutineNumber = 10
)

type Result struct {
	ID    int
	Value int
}

func main() {
	var wg sync.WaitGroup
	numbers := make(chan Result, CountGoroutineNumber)

	for i := 0; i < CountGoroutineNumber; i++ {
		wg.Add(1)
		go func(number chan<- Result, goroutineNumber int) {
			defer wg.Done()
			numbers <- Result{goroutineNumber, goroutineNumber * goroutineNumber}
		}(numbers, i)
	}

	wg.Wait()
	close(numbers)

	for n := range numbers {
		fmt.Printf("Горутина #%d вернула результат = %d\n", n.ID, n.Value)
	}
}

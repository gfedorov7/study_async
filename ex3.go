package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	CountLimitGoroutine = 3
	CountGoroutine      = 10
)

type Result2 struct {
	ID    int
	Value int
	Wait  int
}

func calculate(
	limit chan struct{},
	result chan<- Result2,
	wg *sync.WaitGroup,
	i int,
	ctx context.Context) {
	limit <- struct{}{}
	defer wg.Done()
	defer func() { <-limit }()
	wait := rand.Intn(3)
	select {
	case <-time.After(time.Duration(wait) * time.Second):
		result <- Result2{i, i * i, wait}
	case <-ctx.Done():
		fmt.Println("Завершаем")
		return
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup
	result := make(chan Result2, CountGoroutine)
	limit := make(chan struct{}, CountLimitGoroutine)

	for i := 0; i < CountGoroutine; i++ {
		wg.Add(1)
		go calculate(limit, result, &wg, i, ctx)
	}

	wg.Wait()
	close(result)

	for r := range result {
		fmt.Printf("Горутина #%d вернула результат = %d\n", r.ID, r.Value)
	}
}

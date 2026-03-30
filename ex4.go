package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	CountLimitGoroutine2 = 3
	CountGoroutine2      = 10
)

type Result3 struct {
	ID    int
	Value int
	Wait  int
}

func calculate2(
	limit chan struct{},
	result chan<- Result3,
	wg *sync.WaitGroup,
	i int,
	ctx context.Context) {
	defer wg.Done()

	select {
	case limit <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-limit }()

	wait := rand.Intn(3)

	select {
	case <-time.After(time.Duration(wait) * time.Second):
	case <-ctx.Done():
		return
	}

	select {
	case result <- Result3{i, i * i, wait}:
	case <-ctx.Done():
		return
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup
	result := make(chan Result3, CountGoroutine2)
	limit := make(chan struct{}, CountLimitGoroutine2)

	for i := 0; i < CountGoroutine2; i++ {
		wg.Add(1)
		go calculate2(limit, result, &wg, i, ctx)
	}

	wg.Wait()
	fmt.Println("Завершаем")
	close(result)

	for r := range result {
		fmt.Printf("Горутина #%d вернула результат = %d | (%d s)\n", r.ID, r.Value, r.Wait)
	}
}

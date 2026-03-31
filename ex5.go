package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	CountGoroutineEx5 int = 10
	CountSameEx5      int = 3
)

type ResultEx5 struct {
	ID      int
	Value   int
	Wait    int
	Retries int
}

func worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	result chan ResultEx5,
	jobs chan int,
	limit chan struct{},
	errors chan int,
	retries map[int]int) {
	defer wg.Done()

	for {
		select {
		case limit <- struct{}{}:
		case <-ctx.Done():
			select {
			case <-limit:
				return
			case <-ctx.Done():
				return
			}
		}

		wait := rand.Intn(3)

		select {
		case <-time.After(time.Duration(wait)):
		case <-ctx.Done():
			return
		}

		select {
		case job, ok := <-jobs:
			if !ok {
				select {
				case <-limit:
					return
				case <-ctx.Done():
					return
				}
			}
			if wait == 0 {
				errors <- job
				select {
				case _, ok := <-limit:
					if !ok {
						return
					}
					continue
				case <-ctx.Done():
					return
				}
			}
			result <- ResultEx5{job, job * job, wait, retries[job]}
		case <-ctx.Done():
			select {
			case <-limit:
				return
			case <-ctx.Done():
				return
			}
		}

		select {
		case <-limit:
			return
		case <-ctx.Done():
			return
		}
	}
}

func errorHandler(
	ctx context.Context,
	cancel context.CancelFunc,
	errors chan int,
	jobs chan int,
	retries map[int]int) {

	for {
		select {
		case v, ok := <-errors:
			if !ok {
				return
			}
			if _, ok = retries[v]; !ok {
				retries[v] = 0
			}

			retries[v]++
			if retries[v] >= 2 {
				cancel()
				return
			}

			select {
			case jobs <- v:
			case <-ctx.Done():
				return
			default:
				continue
			}

		case <-ctx.Done():
			return
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	retries := make(map[int]int, CountGoroutineEx5)

	jobs := make(chan int, CountGoroutineEx5)
	result := make(chan ResultEx5, CountGoroutineEx5)
	limit := make(chan struct{}, CountSameEx5)
	errors := make(chan int, CountGoroutineEx5)

	for i := 0; i < CountGoroutineEx5; i++ {
		jobs <- i
	}

	for i := 0; i < CountGoroutineEx5; i++ {
		wg.Add(1)
		go worker(ctx, &wg, result, jobs, limit, errors, retries)
	}

	go errorHandler(ctx, cancel, errors, jobs, retries)

	go func() {
		wg.Wait()
		close(jobs)
		close(errors)
		close(result)
	}()

	for {
		select {
		case r, ok := <-result:
			if !ok {
				return
			}
			fmt.Println(r.ID, r.Value, r.Wait, r.Retries)
		case <-ctx.Done():
			fmt.Println(retries)
			fmt.Println("Финалим")
			return
		}
	}
}

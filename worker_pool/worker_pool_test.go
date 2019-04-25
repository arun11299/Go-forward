package worker_pool

import (
	"fmt"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	pool_ctx := NewPool(int8(5))
	pool_ctx.Run()
	pool_ctx.Enqueue(func() { fmt.Println("Task-1") })
	pool_ctx.Enqueue(func() { fmt.Println("Task-2") })
	pool_ctx.Enqueue(func() { fmt.Println("Task-3") })

	time.Sleep(10 * time.Second)
}

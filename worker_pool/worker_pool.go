package worker_pool

import (
	"fmt"
	"sync"
)

// Job is basically a function which gets executed
// by one of the goroutine in the pool
// type Job func(context interface{})
type Job func()

type JobQueue struct {
	q    []Job
	lock sync.Mutex
	cond *sync.Cond
}

func (self *JobQueue) PopLocked() Job {
	item := self.q[len(self.q)-1]
	self.q = self.q[:len(self.q)-1]
	return item
}

func (self *JobQueue) PushLocked(item Job) {
	self.q = append(self.q, item)
}

// --------------------------------------------------------------

type PoolContext struct {
	num_workers int8
	queue       *JobQueue
}

func NewPool(num_workers int8) PoolContext {
	jqueue := &JobQueue{}
	jqueue.cond = sync.NewCond(&jqueue.lock)

	pool_ctx := PoolContext{
		num_workers: num_workers,
		queue:       jqueue,
	}
	return pool_ctx
}

func (self *PoolContext) Enqueue(job Job) {
	self.queue.lock.Lock()
	self.queue.PushLocked(job)
	self.queue.lock.Unlock()
	self.queue.cond.Broadcast()
}

func (self *PoolContext) Run() {
	for i := int8(0); i < self.num_workers; i++ {
		go self.RunInternal()
	}
}

func (self *PoolContext) RunInternal() {
	fmt.Println("Running worker...")
	for {
		self.queue.lock.Lock()
		for len(self.queue.q) == 0 {
			self.queue.cond.Wait()
		}
		// Process one job and yield
		job := self.queue.PopLocked()
		job()
		self.queue.cond.Wait()
	}
}

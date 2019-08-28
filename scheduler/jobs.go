package scheduler

import (
	"errors"
	"sync"
)

var errPaused = errors.New("job scheduler is paused")
var errFull = errors.New("job scheduler is full")

// Jobs is a structure that holds the current state of the job scheduler.
type Jobs struct {
	currentCap int
	capacity   chan int
	pause      chan bool
	paused     bool
	wg         sync.WaitGroup
	mutex      sync.RWMutex
}

// NewJobScheduler creates a new job scheduler.
// The scheduler can be paused/resumed by sending true/false to the pause channel.
// The capacity of the scheduler can be modified by sending it to the capacity channel.
// The initial capacity value will be passed to the capacity.
func NewJobScheduler(initialCap int, capacity chan int, pause chan bool) Jobs {
	capacity <- initialCap

	return Jobs{
		currentCap: initialCap,
		capacity:   capacity,
		pause:      pause,
		paused:     false,
		wg:         sync.WaitGroup{},
		mutex:      sync.RWMutex{},
	}
}

// Add allocates a job in the job scheduler.
// It will return an error if the scheduler is paused.
// It will return an error if the capacity is full.
func (j *Jobs) Add() error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	if j.paused {
		return errPaused
	}

	if j.currentCap <= 0 {
		return errFull
	}

	j.currentCap--
	j.capacity <- j.currentCap
	j.wg.Add(1)

	return nil
}

// Done frees a job from the job scheduler.
// It will block if no jobs are currently there.
func (j *Jobs) Done() {
	j.mutex.Lock()
	j.currentCap++
	j.mutex.Unlock()

	j.capacity <- j.currentCap
	j.wg.Done()
}

// Pause will pause the scheduler so that new jobs cannot be scheduled.
// Currently running jobs will continue normally.
func (j *Jobs) Pause() {
	j.mutex.Lock()
	j.paused = true
	j.mutex.Unlock()

	j.pause <- true
}

// Resume will resume the scheduler so that new jobs can be scheduled.
func (j *Jobs) Resume() {
	j.mutex.Lock()
	j.paused = false
	j.mutex.Unlock()

	j.pause <- false
}

// Wait will wait until all running jobs are finished.
func (j *Jobs) Wait() {
	j.wg.Wait()
}

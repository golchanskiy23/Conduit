package scheduler

import "errors"

var(
	ErrEmptyQueue = errors.New("priority queue is empty")
	ErrConversion = errors.New("error in type conversion")
	ErrCyclicDependency = errors.New("cycle has been founded")
	ErrAlreadyExists    = errors.New("job already exists")
	ErrPoolClosed       = errors.New("worker pool is closed")
)
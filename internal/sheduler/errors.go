package sheduler

import "errors"

var(
	ErrEmptyQueue = errors.New("priority queue is empty")
	ErrConversation = errors.New("error in type conversation")
	ErrCyclicDependency = errors.New("cycle has been founded")
	ErrAlreadyExists    = errors.New("job already exists")
)
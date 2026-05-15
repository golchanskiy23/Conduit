package sheduler

import "errors"

var(
	ErrEmptyQueue = errors.New("priority queue is empty")
	ErrConversation = errors.New("error in type conversation")
)
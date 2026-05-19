package queue

import "errors"

var(
	ErrEmptyQueue = errors.New("priority queue is empty")
	ErrConversion = errors.New("error in type conversion")
)
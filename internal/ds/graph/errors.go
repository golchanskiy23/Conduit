package graph

import "errors"

var(
	ErrAlreadyExists    = errors.New("job already exists")
	ErrCyclicDependency = errors.New("cycle has been founded")
)
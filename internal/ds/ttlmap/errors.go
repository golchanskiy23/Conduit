package ttlmap

import "errors"

var (
	ErrDeadlineComing = errors.New("run-time point after ttl deadline")
	ErrNoSuchElement = errors.New("no such element ")
)
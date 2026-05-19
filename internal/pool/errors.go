package pool

import "errors"

var ErrPoolClosed       = errors.New("worker pool is closed")

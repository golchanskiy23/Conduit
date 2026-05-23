package scheduler

import "fmt"

var ErrNoSuchWorker = fmt.Errorf("no worker registered for job type")
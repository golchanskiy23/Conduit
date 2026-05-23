package pool

type workerOption func(*WorkerPool)

func WithOnDone(fn func(string)) workerOption {
    return func(o *WorkerPool) {
        o.onDone = fn
    }
}

func WithOnError(fn func(string, error)) workerOption {
    return func(o *WorkerPool) {
        o.onError = fn
    }
}
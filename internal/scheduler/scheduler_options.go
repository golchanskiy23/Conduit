package scheduler

type Option func(*Scheduler)

func WithTaskOnError(fn func(string, error)) Option {
	return func(s *Scheduler) {
		s.onError = fn
	}
}
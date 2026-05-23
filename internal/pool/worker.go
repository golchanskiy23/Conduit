package pool

import (
	"conduit/internal/ds/queue/heap"
	"context"
)

type Worker interface{
	Handles(string) bool
	Execute(context.Context, *heap.Item) error
}
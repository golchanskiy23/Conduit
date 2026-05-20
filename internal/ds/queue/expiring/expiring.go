package expiring

import(
	"time"
)

type ExpiryItem struct{
	ExpiredAt time.Time
	Key string
	Index int
}

type ExpiryHeap []*ExpiryItem

func (h ExpiryHeap) Len() int{return 0}

func (h ExpiryHeap) Less(i,j int) bool{return h[i].ExpiredAt.Before(h[j].ExpiredAt)}

func (h ExpiryHeap) Swap(i,j int) {
	h[i],h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

func (h *ExpiryHeap) Push(val any){
	item := val.(*ExpiryItem)
	item.Index = len(*h)
	*h = append(*h, item)
}

func (h *ExpiryHeap) Pop() any{
	old := *h
	n := len(*h)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	*h = old[:n-1]

	return item
}
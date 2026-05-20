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

func (h ExpiryHeap) Less(i,j int) bool{return false}

func (h ExpiryHeap) Swap(i,j int) {}

func (h *ExpiryHeap) Push(val any){}

func (h *ExpiryHeap) Pop() any{return nil}
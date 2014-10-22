package wfq

import (
	"container/heap"

	"sync"
)

/*****************************************************************************
 *
 *****************************************************************************/

// An implementatino of this interface is passed to NewQueue
// and used to obtain properties of items passed to Queue().
// Each method will be called only once per Queue()/item call
//
type Interface interface {
	// Key returns the identity of the flow for the item.
	// All items with the same key are placed in the same flow.
	//
	Key(item interface{}) uint64

	// Size returns the size of the item.
	// The value returned can be any unit but all items in a queue must be
	// sized according to the same unit (e.g. bytes).
	Size(item interface{}) uint64

	// The weight/priority of the item. A higher value represents a higher
	// priority. All items with a specific key should (but are not required to)
	// have the same weight.
	Weight(item interface{}) uint8
}

/*****************************************************************************
 *
 *****************************************************************************/

type heapItem struct {
	fi     *flowInfo
	value  interface{}
	size   uint64
	weight uint8
	key    uint64
	vft    uint64
}

var hi_pool sync.Pool

func newHeapItem() interface{} {
	return new(heapItem)
}

func getHeapItem() *heapItem {
	return hi_pool.Get().(*heapItem)
}

func putHeapItem(hi *heapItem) {
	hi.fi = nil
	hi.value = nil
	hi_pool.Put(hi)
}

/*****************************************************************************
 *
 *****************************************************************************/

type itemHeap []*heapItem

func (h *itemHeap) Len() int {
	return len(*h)
}

func (h *itemHeap) Less(i, j int) bool {
	return (*h)[i].vft < (*h)[j].vft
}

func (h *itemHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *itemHeap) Push(x interface{}) {
	item := x.(*heapItem)
	*h = append(*h, item)
}

func (h *itemHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	old[n-1] = nil
	return item
}

/*****************************************************************************
 *
 *****************************************************************************/

type overflowHeapItem struct {
	hi     *heapItem
	arrord uint64
	wg     sync.WaitGroup
}

func (i *overflowHeapItem) less(o *overflowHeapItem) bool {
	if i.hi.weight > o.hi.weight {
		return true
	} else if i.hi.weight == o.hi.weight && i.arrord < o.arrord {
		return true
	}
	return false
}

var ohi_pool sync.Pool

func newOverflowHeapItem() interface{} {
	return new(overflowHeapItem)
}

func getOverflowHeapItem() *overflowHeapItem {
	return ohi_pool.Get().(*overflowHeapItem)
}

func putOverflowHeapItem(ohi *overflowHeapItem) {
	ohi.hi = nil
	ohi_pool.Put(ohi)
}

/*****************************************************************************
 *
 *****************************************************************************/

type itemOverflowHeap []*overflowHeapItem

func (h *itemOverflowHeap) Len() int {
	return len(*h)
}

func (h *itemOverflowHeap) Less(i, j int) bool {
	return (*h)[i].less((*h)[j])
}

func (h *itemOverflowHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *itemOverflowHeap) Push(x interface{}) {
	item := x.(*overflowHeapItem)
	*h = append(*h, item)
}

func (h *itemOverflowHeap) Pop() interface{} {
	old := *h
	n := len(old)
	ohi := old[n-1]
	*h = old[0 : n-1]
	old[n-1] = nil
	return ohi
}

/*****************************************************************************
 *
 *****************************************************************************/

type flowInfo struct {
	cond     sync.Cond
	last_vft uint64
	size     uint64
	pendSize uint64
}

var fi_pool sync.Pool

func newFlowInfo() interface{} {
	return new(flowInfo)
}

func getFlowInfo() *flowInfo {
	return fi_pool.Get().(*flowInfo)
}

func putFlowInfo(fi *flowInfo) {
	fi.cond.L = nil
}

/*****************************************************************************
 *
 *****************************************************************************/

func init() {
	hi_pool.New = newHeapItem
	ohi_pool.New = newOverflowHeapItem
	fi_pool.New = newFlowInfo
}

/*****************************************************************************
 *
 *****************************************************************************/

// A queue that implements a version of the weighted fair queue algorithm.
// When all items have the same weight then each flow's throughput will be
// <total throughput>/<number of flows>.
//
// If items have different weights, then all flows with the same weight will
// share their portion of the throughput evenly.
// Each weight "class" receives a portion of the total throughput according to
// the following the formula RWi/W1 + W2 ... + WN where R = total throughput
// and W1 through WN are the weights.
// The actual wieghted cost of an item is calculated as C = (S * 256) / (1 + W)
// where S = the size of the item, and W = the weight of the item.
//
type Queue struct {
	lock         sync.Mutex
	cond         sync.Cond
	closed       bool
	maxQueueSize uint64
	maxFlowSize  uint64
	helper       Interface
	items        itemHeap
	overflow     itemOverflowHeap
	next_ohi     *overflowHeapItem
	flows        map[uint64]*flowInfo
	ovfcnt       uint64
	vt           uint64
	size         uint64
}

// Create a new Queue instance.
// If maxFlowSize > maxQueueSize or if helper is nil then it will panic.
// The maxFlowSize value limits the total size of all items that can be queued in a single flow.
// The maxQueueSize value limits the total size of all items that can be in the queue.
// It is recomeneded that maxQueueSize be set to maxFlowSize*<Max # of flows>, and
// maxFlowSize must be larger than the largest item ti be placed in the queue.
//
func NewQueue(maxQueueSize, maxFlowSize uint64, helper Interface) *Queue {
	if maxFlowSize > maxQueueSize {
		panic("MaxFlowSize > MaxQueueSize")
	}

	if helper == nil {
		panic("helper is nil")
	}

	q := new(Queue)
	q.cond.L = &q.lock
	q.maxQueueSize = maxQueueSize
	q.maxFlowSize = maxFlowSize
	q.helper = helper
	q.flows = make(map[uint64]*flowInfo)

	return q
}

// Place on item on the queue. Queue will not return (i.e. block) until the item can be placed on the queue
// or the queue was closed. If Queue returns true, then DeQueue will eventually return the item.
// If Queue returns false, then the item was not placed on the queue because the queue has been closed.
// Queue will panic if the size of the item is greater then maxFlowSize (set in NewQueue).
// Queue is safe for concurrent use.
//
func (q *Queue) Queue(item interface{}) bool {
	hi := getHeapItem()
	hi.value = item
	hi.key = q.helper.Key(item)
	hi.size = q.helper.Size(item)
	hi.weight = q.helper.Weight(item)

	if hi.size > q.maxFlowSize {
		panic("Item size is larger than MaxFlowSize")
	}

	q.lock.Lock()

	if q.closed {
		q.lock.Unlock()
		return false
	}

	// Get the flowInfo, or add one if there is none
	fi, ok := q.flows[hi.key]
	if !ok {
		fi = getFlowInfo()
		fi.cond.L = &q.lock
		fi.last_vft = q.vt
		q.flows[hi.key] = fi
	}
	hi.fi = fi

	// This prevents DeQueue from deleting the flowInfo from q.flows
	// while the flow is till active
	fi.pendSize += hi.size

	// Wait till there is room in the flow queue
	for !q.closed && fi.size+hi.size > q.maxFlowSize {
		fi.cond.Wait()
	}

	if q.closed {
		q.lock.Unlock()
		return false
	}

	// Calculate the items virtual finish time
	hi.vft = fi.last_vft + ((hi.size << 8) / (uint64(hi.weight) + 1))
	fi.last_vft = hi.vft

	// Add the item's size to the flow
	fi.size += hi.size
	// Subtract it's size from pendSize since it is no longer pending
	fi.pendSize -= hi.size

	if q.size+hi.size > q.maxQueueSize {
		/*
			The queue is full, place our request in the overflow heap.
			Unlike the main heap, the overflow heap is strictly prioritized by
			weight and arrival order. A higher priority flow could completely starve out
			a lower priority flow if the incoming rate of the higer priority flow exceeds
			the total outgoing rate.
		*/
		ohi := getOverflowHeapItem()
		ohi.hi = hi
		ohi.arrord = q.ovfcnt
		q.ovfcnt++
		ohi.wg.Add(1)
		if q.next_ohi == nil {
			q.next_ohi = ohi
		} else {
			if ohi.less(q.next_ohi) {
				heap.Push(&q.overflow, q.next_ohi)
				q.next_ohi = ohi
			} else {
				heap.Push(&q.overflow, ohi)
			}
		}
		q.lock.Unlock()
		ohi.wg.Wait()
		putOverflowHeapItem(ohi)
		if q.closed {
			return false
		}
	} else {
		q.size += hi.size
		// The queue has room, place our item in the main heap
		heap.Push(&q.items, hi)
		q.cond.Signal()
		q.lock.Unlock()
	}

	return true
}

// DeQueue removes the next item from the queue. DeQueue will not return (i.e. block) until an item can
// be returned or the queue is empty and closed. DeQueue will return an item and true if an item could be
// removed from the queue or nil and false, if the queue is empty and closed.
// DeQueue is safe for concurrent use.
//
func (q *Queue) DeQueue() (interface{}, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.closed && q.items.Len() == 0 {
		return nil, false
	}

	for !q.closed && q.items.Len() == 0 {
		q.cond.Wait()
	}

	if q.closed && q.items.Len() == 0 {
		return nil, false
	}

	hi := heap.Pop(&q.items).(*heapItem)
	item := hi.value
	q.vt = hi.vft
	hi.fi.size -= hi.size
	q.size -= hi.size
	if hi.fi.size == 0 && hi.fi.pendSize == 0 {
		// The flow is empty (i.e. inactive), delete it
		delete(q.flows, hi.key)
		putFlowInfo(hi.fi)
		putHeapItem(hi)
	} else {
		hi.fi.cond.Signal()
		putHeapItem(hi)
	}

	if !q.closed {
		// While there is room in the queue move items from the overflow to the main heap.
		for q.next_ohi != nil && q.size+q.next_ohi.hi.size <= q.maxQueueSize {
			q.size += q.next_ohi.hi.size
			heap.Push(&q.items, q.next_ohi.hi)
			q.next_ohi.wg.Done()
			if q.overflow.Len() > 0 {
				q.next_ohi = heap.Pop(&q.overflow).(*overflowHeapItem)
			} else {
				q.next_ohi = nil
			}
		}
	}

	return item, true
}

func (q *Queue) Close() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.closed = true
	// All overflow requests get flushed
	for q.next_ohi != nil {
		q.next_ohi.wg.Done()
		q.next_ohi = q.overflow.Pop().(*overflowHeapItem)
	}
	// Wake up all those waiting to get into a flow queue
	for _, fi := range q.flows {
		fi.cond.Broadcast()
	}
	// Wake up all DeQueue'ers
	q.cond.Broadcast()
}

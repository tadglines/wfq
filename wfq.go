package wfq

import (
	"container/heap"

	"sync"
)

/*****************************************************************************
 *
 *****************************************************************************/

// An implementatino of this interface is passed to NewWeightedFairQueue
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

type queueItem struct {
	fi     *flowInfo
	value  interface{}
	size   uint64
	weight uint8
	key    uint64
	vft    uint64
}

var qi_pool sync.Pool

func newQueueItem() interface{} {
	return new(queueItem)
}

func getQueueItem() *queueItem {
	return qi_pool.Get().(*queueItem)
}

func putQueueItem(qi *queueItem) {
	qi.fi = nil
	qi.value = nil
	qi_pool.Put(qi)
}

/*****************************************************************************
 *
 *****************************************************************************/

type itemQueue []*queueItem

func (q *itemQueue) Len() int {
	return len(*q)
}

func (q *itemQueue) Less(i, j int) bool {
	return (*q)[i].vft < (*q)[j].vft
}

func (q *itemQueue) Swap(i, j int) {
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
}

func (q *itemQueue) Push(x interface{}) {
	item := x.(*queueItem)
	*q = append(*q, item)
}

func (q *itemQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	old[n-1] = nil
	return item
}

/*****************************************************************************
 *
 *****************************************************************************/

type overflowQueueItem struct {
	qi     *queueItem
	arrord uint64
	wg     sync.WaitGroup
}

func (i *overflowQueueItem) less(o *overflowQueueItem) bool {
	if i.qi.weight > o.qi.weight {
		return true
	} else if i.qi.weight == o.qi.weight && i.arrord < o.arrord {
		return true
	}
	return false
}

var oqi_pool sync.Pool

func newOverflowQueueItem() interface{} {
	return new(overflowQueueItem)
}

func getOverflowQueueItem() *overflowQueueItem {
	return oqi_pool.Get().(*overflowQueueItem)
}

func putOverflowQueueItem(oqi *overflowQueueItem) {
	oqi.qi = nil
	oqi_pool.Put(oqi)
}

/*****************************************************************************
 *
 *****************************************************************************/

type itemOverflowQueue []*overflowQueueItem

func (q *itemOverflowQueue) Len() int {
	return len(*q)
}

func (q *itemOverflowQueue) Less(i, j int) bool {
	return (*q)[i].less((*q)[j])
}

func (q *itemOverflowQueue) Swap(i, j int) {
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
}

func (q *itemOverflowQueue) Push(x interface{}) {
	item := x.(*overflowQueueItem)
	*q = append(*q, item)
}

func (q *itemOverflowQueue) Pop() interface{} {
	old := *q
	n := len(old)
	oqi := old[n-1]
	*q = old[0 : n-1]
	old[n-1] = nil
	return oqi
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
	qi_pool.New = newQueueItem
	oqi_pool.New = newOverflowQueueItem
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
type WeightedFairQueue struct {
	lock         sync.Mutex
	cond         sync.Cond
	closed       bool
	maxQueueSize uint64
	maxFlowSize  uint64
	helper       Interface
	items        itemQueue
	overflow     itemOverflowQueue
	next_oi      *overflowQueueItem
	flows        map[uint64]*flowInfo
	ovfcnt       uint64
	vt           uint64
	size         uint64
}

// Create a new WeightedFairQueue instance.
// If maxFlowSize > maxQueueSize or if helper is nil then it will panic.
//
func NewWeightedFairQueue(maxQueueSize, maxFlowSize uint64, helper Interface) *WeightedFairQueue {
	if maxFlowSize > maxQueueSize {
		panic("MaxFlowSize > MaxQueueSize")
	}

	if helper == nil {
		panic("h is nil")
	}

	q := new(WeightedFairQueue)
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
// Queue is safe for concurrent use.
//
func (q *WeightedFairQueue) Queue(item interface{}) bool {
	qi := getQueueItem()
	qi.value = item
	qi.key = q.helper.Key(item)
	qi.size = q.helper.Size(item)
	qi.weight = q.helper.Weight(item)

	if qi.size > q.maxFlowSize {
		panic("Item size is larger than MaxFlowSize")
	}

	q.lock.Lock()

	if q.closed {
		q.lock.Unlock()
		return false
	}

	// Get the flowInfo, or add one if there is none
	fi, ok := q.flows[qi.key]
	if !ok {
		fi = getFlowInfo()
		fi.cond.L = &q.lock
		fi.last_vft = q.vt
		q.flows[qi.key] = fi
	}
	qi.fi = fi

	// This prevents DeQueue from deleting the flowInfo from q.flows
	// while the flow is till active
	fi.pendSize += qi.size

	// Wait till there is room in the flow queue
	for !q.closed && fi.size+qi.size > q.maxFlowSize {
		fi.cond.Wait()
	}

	if q.closed {
		q.lock.Unlock()
		return false
	}

	// Calculate the items virtual finish time
	qi.vft = fi.last_vft + ((qi.size << 8) / (uint64(qi.weight) + 1))
	fi.last_vft = qi.vft

	// Add the item's size to the flow
	fi.size += qi.size
	// Subtract it's size from pendSize since it is no longer pending
	fi.pendSize -= qi.size

	if q.size+qi.size > q.maxQueueSize {
		/*
			The queue is full, place our request in the overflow heap.
			Unlike the main heap, the overflow heap is strictly prioritized by
			weight and arrival order. A higher priority flow could completely starve out
			a lower priority flow if the incoming rate of the higer priority flow exceeds
			the total outgoing rate.
		*/
		oqi := getOverflowQueueItem()
		oqi.qi = qi
		oqi.arrord = q.ovfcnt
		q.ovfcnt++
		oqi.wg.Add(1)
		if q.next_oi == nil {
			q.next_oi = oqi
		} else {
			if oqi.less(q.next_oi) {
				heap.Push(&q.overflow, q.next_oi)
				q.next_oi = oqi
			} else {
				heap.Push(&q.overflow, oqi)
			}
		}
		q.lock.Unlock()
		oqi.wg.Wait()
		putOverflowQueueItem(oqi)
		if q.closed {
			return false
		}
	} else {
		q.size += qi.size
		// The queue has room, place our item in the main heap
		heap.Push(&q.items, qi)
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
func (q *WeightedFairQueue) DeQueue() (interface{}, bool) {
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

	qi := heap.Pop(&q.items).(*queueItem)
	item := qi.value
	q.vt = qi.vft
	qi.fi.size -= qi.size
	q.size -= qi.size
	if qi.fi.size == 0 && qi.fi.pendSize == 0 {
		// The flow is empty (i.e. inactive), delete it
		delete(q.flows, qi.key)
		putFlowInfo(qi.fi)
		putQueueItem(qi)
	} else {
		qi.fi.cond.Signal()
		putQueueItem(qi)
	}

	if !q.closed {
		// While there is room in the queue move items from the overflow to the main heap.
		for q.next_oi != nil && q.size+q.next_oi.qi.size <= q.maxQueueSize {
			q.size += q.next_oi.qi.size
			heap.Push(&q.items, q.next_oi.qi)
			q.next_oi.wg.Done()
			if q.overflow.Len() > 0 {
				q.next_oi = heap.Pop(&q.overflow).(*overflowQueueItem)
			} else {
				q.next_oi = nil
			}
		}
	}

	return item, true
}

func (q *WeightedFairQueue) Close() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.closed = true
	// All overflow requests get flushed
	for q.next_oi != nil {
		q.next_oi.wg.Done()
		q.next_oi = q.overflow.Pop().(*overflowQueueItem)
	}
	// Wake up all those waiting to get into a flow queue
	for _, fi := range q.flows {
		fi.cond.Broadcast()
	}
	// Wake up all DeQueue'ers
	q.cond.Broadcast()
}

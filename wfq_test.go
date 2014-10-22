package wfq

import (
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

type item struct {
	key    uint64
	size   uint64
	weight uint8
	seq    uint64
}

type helper struct{}

func (h *helper) Key(i interface{}) uint64 {
	return i.(*item).key
}

func (h *helper) Size(i interface{}) uint64 {
	return i.(*item).size
}

func (h *helper) Weight(i interface{}) uint8 {
	return i.(*item).weight
}

func TestSingleFlow(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewWeightedFairQueue(1000, 10, &helper{})

	go func() {
		for i := 1; i < 10000; i++ {
			it := new(item)
			it.key = 1
			it.size = uint64(rand.Int63n(10) + 1)
			it.weight = 1
			it.seq = uint64(i)
			wfq.Queue(it)
		}
		wfq.Close()
	}()

	var seq uint64
	for it, ok := wfq.DeQueue(); ok; it, ok = wfq.DeQueue() {
		if seq+1 != it.(*item).seq {
			t.Fatalf("Item came out of queue out-of-order: expected %d, got %d", seq+1, it.(*item).seq)
		}
		seq = it.(*item).seq
	}
}

func TestMultiFlow(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewWeightedFairQueue(100, 10, &helper{})

	var swg sync.WaitGroup
	var wg sync.WaitGroup
	numFlows := 20
	swg.Add(1)
	wg.Add(numFlows)
	for n := 0; n < numFlows; n++ {
		go func(key uint64) {
			swg.Wait()
			for i := 1; i < 100; i++ {
				it := new(item)
				it.key = key
				it.size = 1
				it.weight = 0
				it.seq = uint64(i)
				wfq.Queue(it)
			}
			wg.Done()
		}(uint64(n))
	}

	go func() {
		wg.Wait()
		wfq.Close()
	}()
	swg.Done()

	time.Sleep(time.Millisecond)

	var tick uint64 = 1
	lastTicks := make(map[uint64]uint64)
	deltas := make(map[uint64][]uint64)
	seqs := make(map[uint64]uint64)

	for i, ok := wfq.DeQueue(); ok; i, ok = wfq.DeQueue() {
		it := i.(*item)
		seq := seqs[it.key]
		if seq+1 != it.seq {
			t.Fatalf("Item came out of queue out-of-order: expected %d, got %d", seq+1, it.seq)
		}
		seqs[it.key] = it.seq
		lastTick := lastTicks[it.key]
		if lastTick != 0 {
			deltas[it.key] = append(deltas[it.key], tick-lastTick)
		}
		lastTicks[it.key] = tick
		tick++
	}

	deltaAvgs := make(map[uint64]float64)
	for n := uint64(0); n < uint64(numFlows); n++ {
		var avg float64
		for _, d := range deltas[n] {
			avg += float64(d)
		}
		avg /= float64(len(deltas[n]))
		deltaAvgs[n] = avg
	}

	var avg float64
	for _, a := range deltaAvgs {
		avg += a
	}
	avg /= float64(numFlows)
	var variance float64
	for _, a := range deltaAvgs {
		x := float64(numFlows) - a
		x *= x
		variance += x
	}
	variance /= float64(numFlows)
	stdDev := math.Sqrt(variance)

	// Typically the StdDev will be less than 0.1 but it is still possible
	// to occasionally see a value higher than 0.5

	if stdDev > 0.5 {
		t.Fatalf("StdDev was expected to be < 0.5 but got %v", stdDev)
	}
}

func TestMultiFlowWeighted(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewWeightedFairQueue(1000, 12, &helper{})

	var wg sync.WaitGroup
	numFlows := 10
	wg.Add(numFlows + 1)
	for n := 0; n < numFlows; n++ {
		go func(key uint64) {
			for i := 1; i < 100; i++ {
				it := new(item)
				it.key = key
				it.size = 4
				it.weight = 0
				it.seq = uint64(i)
				wfq.Queue(it)
			}
			wg.Done()
		}(uint64(n))
	}

	go func(key uint64) {
		for i := 1; i < 200; i++ {
			it := new(item)
			it.key = key
			it.size = 1
			it.weight = 255
			it.seq = uint64(i)
			wfq.Queue(it)
		}
		wg.Done()
	}(uint64(11))

	go func() {
		wg.Wait()
		wfq.Close()
	}()

	seqs := make(map[uint64]uint64)
	ft := make(map[uint64]time.Time)
	for i, ok := wfq.DeQueue(); ok; i, ok = wfq.DeQueue() {
		it := i.(*item)
		seq := seqs[it.key]
		if seq+1 != it.seq {
			t.Fatalf("Item came out of queue out-of-order: expected %d, got %d", seq+1, it.seq)
		}
		seqs[it.key] = it.seq
		ft[it.key] = time.Now()
	}
	for n := uint64(0); n < uint64(numFlows); n++ {
		if ft[n].Before(ft[11]) {
			t.Fatalf("High priority flow finished %v AFTER low priority flow", ft[11].Sub(ft[n]))
		}
	}
}

func TestMultiFlowMultiWeight(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewWeightedFairQueue(100, 10, &helper{})

	var swg sync.WaitGroup
	var wg sync.WaitGroup
	numFlows := 20
	swg.Add(1)
	wg.Add(numFlows)
	for n := 0; n < numFlows; n++ {
		go func(key uint64) {
			swg.Wait()
			for i := 1; i < 100; i++ {
				it := new(item)
				it.key = key
				it.size = 1
				it.weight = uint8(n)
				it.seq = uint64(i)
				wfq.Queue(it)
			}
			wg.Done()
		}(uint64(n))
	}

	go func() {
		wg.Wait()
		wfq.Close()
	}()
	swg.Done()

	time.Sleep(time.Millisecond)

	seqs := make(map[uint64]uint64)
	for i, ok := wfq.DeQueue(); ok; i, ok = wfq.DeQueue() {
		it := i.(*item)
		seq := seqs[it.key]
		if seq+1 != it.seq {
			t.Fatalf("Item came out of queue out-of-order: expected %d, got %d", seq+1, it.seq)
		}
		seqs[it.key] = it.seq
	}
}

func TestClose(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewWeightedFairQueue(100, 10, &helper{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 1; i < 1000; i++ {
			it := new(item)
			it.key = 1
			it.size = uint64(rand.Int63n(10) + 1)
			it.weight = 1
			it.seq = uint64(i)
			ok := wfq.Queue(it)
			if !ok {
				break
			}
		}
		wg.Done()
	}()

	var seq uint64
	for it, ok := wfq.DeQueue(); ok && seq < 100; it, ok = wfq.DeQueue() {
		if seq+1 != it.(*item).seq {
			t.Fatalf("Item came out of queue out-of-order: expected %d, got %d", seq+1, it.(*item).seq)
		}
		seq = it.(*item).seq
	}

	wfq.Close()

	wg.Wait()

	if wfq.Queue(&item{key: 1, size: 1, weight: 1}) != false {
		t.Fatal("Queue didn't return false")
	}
}

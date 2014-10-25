package wfq

import (
	"fmt"
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

func (i *item) String() string {
	return fmt.Sprintf("{%d %d %d %d}", i.key, i.size, i.weight, i.seq)
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

type flowDesc struct {
	// In
	ftotal uint64 // Total units in flow
	imin   uint64 // Min item size
	imax   uint64 // Max item size
	weight uint8  // Flow weight

	// Out
	idealPercent  float64
	actualPercent float64
}

func genFlow(queue *Queue, desc *flowDesc, key uint64, done_wg *sync.WaitGroup) {
	for i, t := uint64(1), uint64(0); t < desc.ftotal; i++ {
		//time.Sleep(time.Microsecond)
		it := new(item)
		it.key = key
		if desc.imin == desc.imax {
			it.size = desc.imax
		} else {
			it.size = desc.imin + uint64(rand.Int63n(int64(desc.imax-desc.imin)))
		}
		if t+it.size > desc.ftotal {
			it.size = desc.ftotal - t
		}
		t += it.size
		it.weight = desc.weight
		it.seq = i
		queue.Queue(it)
	}
	(*done_wg).Done()
}

func consumeQueue(queue *Queue, descs []flowDesc) (float64, error) {
	active := make(map[uint64]bool)
	var total uint64
	acnt := make(map[uint64]uint64)
	cnt := make(map[uint64]uint64)
	seqs := make(map[uint64]uint64)

	var wsum uint64
	for _, d := range descs {
		wsum += uint64(d.weight + 1)
	}

	for i, ok := queue.DeQueue(); ok; i, ok = queue.DeQueue() {
		time.Sleep(time.Microsecond) // Simulate constrained bandwidth
		it := i.(*item)
		seq := seqs[it.key]
		if seq+1 != it.seq {
			return 0, fmt.Errorf("Item for flow %d came out of queue out-of-order: expected %d, got %d", it.key, seq+1, it.seq)
		}
		seqs[it.key] = it.seq

		if cnt[it.key] == 0 {
			active[it.key] = true
		}
		cnt[it.key] += it.size

		if len(active) == len(descs) {
			acnt[it.key] += it.size
			total += it.size
		}

		if cnt[it.key] == descs[it.key].ftotal {
			delete(active, it.key)
		}
	}

	var variance float64
	for key := uint64(0); key < uint64(len(descs)); key++ {
		descs[key].idealPercent = (((float64(total) * float64(descs[key].weight+1)) / float64(wsum)) / float64(total)) * 100
		descs[key].actualPercent = (float64(acnt[key]) / float64(total)) * 100
		x := descs[key].idealPercent - descs[key].actualPercent
		x *= x
		variance += x
	}

	stdDev := math.Sqrt(variance)
	return stdDev, nil
}

func TestSingleFlow(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewQueue(1000, 10, &helper{})

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

func TestUniformMultiFlow(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewQueue(1000, 10, &helper{})

	var swg sync.WaitGroup
	var wg sync.WaitGroup
	var flows = []flowDesc{
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
	}

	swg.Add(1)
	wg.Add(len(flows))
	for n := 0; n < len(flows); n++ {
		go genFlow(wfq, &flows[n], uint64(n), &wg)
	}

	go func() {
		wg.Wait()
		wfq.Close()
	}()
	swg.Done()

	stdDev, err := consumeQueue(wfq, flows)

	if err != nil {
		t.Fatal(err.Error())
	}

	if stdDev > 0.1 {
		for k, d := range flows {
			t.Logf("For flow %d: Expected %v%%, got %v%%", k, d.idealPercent, d.actualPercent)
		}
		t.Fatalf("StdDev was expected to be < 0.1 but got %v", stdDev)
	}
}

func TestUniformMultiFlowWithRandomItemSize(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewQueue(1000, 20, &helper{})

	var swg sync.WaitGroup
	var wg sync.WaitGroup
	var flows = []flowDesc{
		// ftotal, imin, imax, weight, ideal, actual
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
		{10000, 1, 10, 0, 0, 0},
	}

	swg.Add(1)
	wg.Add(len(flows))
	for n := 0; n < len(flows); n++ {
		go genFlow(wfq, &flows[n], uint64(n), &wg)
	}

	go func() {
		wg.Wait()
		wfq.Close()
	}()
	swg.Done()

	stdDev, err := consumeQueue(wfq, flows)

	if err != nil {
		t.Fatal(err.Error())
	}

	if stdDev > 0.1 {
		for k, d := range flows {
			t.Logf("For flow %d: Expected %v%%, got %v%%", k, d.idealPercent, d.actualPercent)
		}
		t.Fatalf("StdDev was expected to be < 0.1 but got %v", stdDev)
	}
}

func TestMultiFlowWithOneHiPriFlow(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewQueue(1000, 10, &helper{})

	var swg sync.WaitGroup
	var wg sync.WaitGroup
	var flows = []flowDesc{
		{10000, 1, 1, 32, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
		{1000, 1, 1, 0, 0, 0},
	}

	swg.Add(1)
	wg.Add(len(flows))
	for n := 0; n < len(flows); n++ {
		go genFlow(wfq, &flows[n], uint64(n), &wg)
	}

	go func() {
		wg.Wait()
		wfq.Close()
	}()
	swg.Done()

	stdDev, err := consumeQueue(wfq, flows)

	if err != nil {
		t.Fatal(err.Error())
	}

	if stdDev > 0.1 {
		for k, d := range flows {
			t.Logf("For flow %d: Expected %v%%, got %v%%", k, d.idealPercent, d.actualPercent)
		}
		t.Fatalf("StdDev was expected to be < 0.1 but got %v", stdDev)
	}
}

func TestMixedWeightMultiFlowWithRandomItemSize(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewQueue(1000, 20, &helper{})

	var swg sync.WaitGroup
	var wg sync.WaitGroup
	var flows = []flowDesc{
		// ftotal, imin, imax, weight, ideal, actual
		{10000, 1, 10, 1, 0, 0},
		{10000, 1, 10, 10, 0, 0},
		{10000, 1, 10, 3, 0, 0},
		{10000, 1, 10, 1, 0, 0},
		{10000, 1, 10, 1, 0, 0},
		{10000, 1, 10, 4, 0, 0},
		{10000, 1, 10, 1, 0, 0},
		{10000, 1, 10, 1, 0, 0},
		{10000, 1, 10, 7, 0, 0},
		{10000, 1, 10, 1, 0, 0},
	}

	swg.Add(1)
	wg.Add(len(flows))
	for n := 0; n < len(flows); n++ {
		go genFlow(wfq, &flows[n], uint64(n), &wg)
	}

	go func() {
		wg.Wait()
		wfq.Close()
	}()
	swg.Done()

	stdDev, err := consumeQueue(wfq, flows)

	if err != nil {
		t.Fatal(err.Error())
	}

	if stdDev > 0.1 {
		for k, d := range flows {
			t.Logf("For flow %d: Expected %v%%, got %v%%", k, d.idealPercent, d.actualPercent)
		}
		t.Fatalf("StdDev was expected to be < 0.1 but got %v", stdDev)
	}
}

func TestClose(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wfq := NewQueue(100, 10, &helper{})

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

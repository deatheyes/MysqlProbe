package util

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

type bucket struct {
	startTime time.Time        // the time this bucket activated
	counters  map[string]int64 // counters
}

func newBucket(startTime time.Time) *bucket {
	return &bucket{
		startTime: startTime,
		counters:  make(map[string]int64),
	}
}

type buckets struct {
	ring                    []*bucket // data slice
	pos                     int       // last bucket
	bucketSizeInNanoseconds int64
	numberOfBuckets         int64
	timeInNanoseconds       int64
}

func newBuckets(numberOfBuckets, timeInNanoseconds int64) *buckets {
	b := &buckets{
		ring:                    make([]*bucket, numberOfBuckets),
		pos:                     0,
		timeInNanoseconds:       timeInNanoseconds,
		bucketSizeInNanoseconds: timeInNanoseconds / numberOfBuckets,
		numberOfBuckets:         numberOfBuckets,
	}

	b.ring[0] = newBucket(time.Now())

	return b
}

func (b *buckets) peekLast() *bucket {
	currentTime := time.Now()
	posInc := currentTime.Sub(b.ring[b.pos].startTime).Nanoseconds() / b.bucketSizeInNanoseconds
	currentPos := int((int64(b.pos) + posInc) % b.numberOfBuckets)
	gap := currentTime.Sub(b.ring[b.pos].startTime).Nanoseconds() % b.bucketSizeInNanoseconds
	if b.ring[currentPos] == nil || currentTime.Sub(b.ring[currentPos].startTime).Nanoseconds() > b.bucketSizeInNanoseconds {
		// empty bucket or a expired bucket
		b.ring[currentPos] = newBucket(currentTime.Add(-time.Duration(gap)))
	}
	b.pos = currentPos
	return b.ring[b.pos]
}

func (b *buckets) Add(key string, inc int64) {
	bucket := b.peekLast()
	bucket.counters[key] += int64(inc)
}

func (b *buckets) Sum(key string) int64 {
	var count int64
	currentTime := time.Now()
	for _, v := range b.ring {
		if v == nil {
			continue
		}

		if currentTime.Sub(v.startTime).Nanoseconds() > b.timeInNanoseconds {
			// expired bucket
			v = nil
			continue
		}

		if _, ok := v.counters[key]; ok {
			count += v.counters[key]
		}
	}
	return count
}

type rollingNumberEvent struct {
	key string
	val int64
	op  string
}

// RollingNumber is struct for statistics
type RollingNumber struct {
	timeInMilliseconds      int64                    // assemble interval
	numberOfBuckets         int64                    // assemble accuracy
	bucketSizeInMillseconds int64                    // time span for each bucket
	buckets                 *buckets                 // data slices
	notify                  chan *rollingNumberEvent // channel to create a operation
	result                  chan int64               // channel to get operation result
}

// NewRollingNumber create a new rolling number
func NewRollingNumber(timeInMilliseconds, numberOfBuckets int64) (*RollingNumber, error) {
	if timeInMilliseconds%numberOfBuckets != 0 {
		return nil, errors.New("The timeInMilliseconds must divide equally into numberOfBuckets")
	}

	n := &RollingNumber{
		timeInMilliseconds:      timeInMilliseconds,
		numberOfBuckets:         numberOfBuckets,
		bucketSizeInMillseconds: timeInMilliseconds / numberOfBuckets,
		buckets:                 newBuckets(numberOfBuckets, timeInMilliseconds*1000000),
		notify:                  make(chan *rollingNumberEvent),
		result:                  make(chan int64),
	}
	go n.process()
	return n, nil
}

func (n *RollingNumber) process() {
	for {
		event := <-n.notify
		switch event.op {
		case "add":
			n.buckets.Add(event.key, event.val)
		case "sum":
			n.result <- n.buckets.Sum(event.key)
		}
	}
}

// Add increase the counter specified by key with val
func (n *RollingNumber) Add(key string, val int64) {
	n.notify <- &rollingNumberEvent{
		key: key,
		val: val,
		op:  "add",
	}
}

// Sum return the sum of the counter sepecified by key
func (n *RollingNumber) Sum(key string) int64 {
	n.notify <- &rollingNumberEvent{
		key: key,
		op:  "sum",
	}
	return <-n.result
}

// AverageInSecond caculate the average of the item specified by key in second
func (n *RollingNumber) AverageInSecond(key string) int64 {
	return n.Sum(key) * 1000 / n.timeInMilliseconds
}

type rangeRing struct {
	list     []int64   // value ring
	seq      []int     // pos sequence
	pos      int       // next pos
	size     int       // ring size mirror from RollingRange
	pos99    int       // 99 quantile pos
	lastseen time.Time // lastupdate time
}

func newRangeRing(size int) *rangeRing {
	r := &rangeRing{
		list: make([]int64, size),
		seq:  make([]int, size),
		pos:  0,
		size: size,
	}
	if r.size < 0 {
		r.size = 1
	}
	r.pos99 = int(math.Ceil(float64(r.size)*0.99)) - 1
	for i := 0; i < r.size; i++ {
		r.seq[i] = i
	}
	return r
}

func (r *rangeRing) getValueBySeq(seq int) int64 {
	return r.list[seq]
}

func (r *rangeRing) Len() int {
	return r.size
}

func (r *rangeRing) Less(i, j int) bool {
	return r.getValueBySeq(r.seq[i]) < r.getValueBySeq(r.seq[j])
}

func (r *rangeRing) Swap(i, j int) {
	r.seq[i], r.seq[j] = r.seq[j], r.seq[i]
}

func (r *rangeRing) min() int64 {
	return r.getValueBySeq(r.seq[0])
}

func (r *rangeRing) max() int64 {
	return r.getValueBySeq(r.seq[r.size-1])
}

func (r *rangeRing) r99() int64 {
	return r.getValueBySeq(r.seq[r.pos99])
}

func (r *rangeRing) add(value int64) {
	r.list[r.pos] = value
	sort.Sort(r)
	r.pos = (r.pos + 1) % r.size
	r.lastseen = time.Now()
}

// RollingRange track the range info in time
type RollingRange struct {
	size       int                   // ring size
	rings      map[string]*rangeRing // value ring
	expiration time.Duration         // key expiration time
	sync.RWMutex
}

// NewRollingRange create a RollingRange object
func NewRollingRange(size int, expiration time.Duration) *RollingRange {
	r := &RollingRange{
		size:       size,
		rings:      make(map[string]*rangeRing),
		expiration: expiration,
	}
	go r.run()
	return r
}

const interval = time.Second

func (r *RollingRange) run() {
	ticker := time.NewTicker(interval)
	for {
		<-ticker.C
		for k, v := range r.rings {
			if time.Now().Sub(v.lastseen) > r.expiration {
				r.Lock()
				delete(r.rings, k)
				r.Unlock()
			}
		}
	}
}

// Add add the value to a ring specified by the key
func (r *RollingRange) Add(key string, value int64) {
	r.RLock()
	defer r.RUnlock()

	if r.rings[key] == nil {
		r.rings[key] = newRangeRing(r.size)
	}
	r.rings[key].add(value)
}

// Min return the lower boundary of a ring specified by the key
func (r *RollingRange) Min(key string) int64 {
	r.RLock()
	defer r.RUnlock()

	if r.rings[key] == nil {
		return 0
	}
	return r.rings[key].min()
}

// Max return the uppper boundary of a ring specified by the key
func (r *RollingRange) Max(key string) int64 {
	r.RLock()
	defer r.RUnlock()

	if r.rings[key] == nil {
		return 0
	}
	return r.rings[key].max()
}

// R99 return the value of 99 quantile
func (r *RollingRange) R99(key string) int64 {
	if r.size == 0 {
		return 0
	}

	if r.rings[key] == nil {
		return 0
	}
	return r.rings[key].r99()
}

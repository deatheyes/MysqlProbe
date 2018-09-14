package util

import (
	"errors"
	"sync"
	"time"

	"github.com/bmizerany/perks/quantile"
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

type quantileCell struct {
	stream   *quantile.Stream
	lastseen time.Time
}

func newQuantileCell() *quantileCell {
	return &quantileCell{
		stream: quantile.NewTargeted(0.001, 0.99, 0.999),
	}
}

func (q *quantileCell) min() int64 {
	return int64(q.stream.Query(0.001))
}

func (q *quantileCell) max() int64 {
	return int64(q.stream.Query(0.999))
}

func (q *quantileCell) r99() int64 {
	return int64(q.stream.Query(0.99))
}

func (q *quantileCell) add(v int64) {
	q.lastseen = time.Now()
	q.stream.Insert(float64(v))
}

// Quantile track the min, max and quantile
type Quantile struct {
	streams    map[string]*quantileCell // quantile map
	expiration time.Duration            // key expiration time
	sync.RWMutex
}

// NewQuantile create a quantile map
func NewQuantile(expiration time.Duration) *Quantile {
	q := &Quantile{
		streams:    make(map[string]*quantileCell),
		expiration: expiration,
	}
	go q.run()
	return q
}

const interval = time.Second * 10

func (q *Quantile) run() {
	ticker := time.NewTimer(interval)
	defer ticker.Stop()

	for {
		<-ticker.C
		q.Lock()
		for k, v := range q.streams {
			if time.Now().Sub(v.lastseen) > q.expiration {
				delete(q.streams, k)
			}
		}
		q.Unlock()
	}
}

// Add add the value to a ring specified by the key
func (q *Quantile) Add(key string, value int64) {
	q.RLock()
	defer q.RUnlock()

	if q.streams[key] == nil {
		q.streams[key] = newQuantileCell()
	}
	q.streams[key].add(value)
}

// Min return the lower boundary of a ring specified by the key
func (q *Quantile) Min(key string) int64 {
	q.RLock()
	defer q.RUnlock()

	if q.streams[key] == nil {
		return 0
	}
	return q.streams[key].min()
}

// Max return the upper boundary of a ring specified by the key
func (q *Quantile) Max(key string) int64 {
	q.RLock()
	defer q.RUnlock()

	if q.streams[key] == nil {
		return 0
	}
	return q.streams[key].max()
}

func (q *Quantile) R99(key string) int64 {
	q.RLock()
	defer q.RUnlock()

	if q.streams[key] == nil {
		return 0
	}
	return q.streams[key].r99()
}

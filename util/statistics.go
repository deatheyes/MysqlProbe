package util

import (
	"errors"
	"time"
)

type bucket struct {
	startTime time.Time        // the time this bucket activated
	counters  map[string]int64 // counters
}

func newBucket(startTime time.Time) *bucket {
	return &bucket{startTime: startTime}
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
	}
	go n.process()
	return n, nil
}

func (n *RollingNumber) process() {
	for {
		select {
		case event := <-n.notify:
			switch event.op {
			case "add":
				n.buckets.Add(event.key, event.val)
			case "sum":
				n.result <- n.buckets.Sum(event.key)
			}
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

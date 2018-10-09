package util

import (
	"errors"
	"math"
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

type point struct {
	value     int64
	timestamp time.Time
}

type quantile struct {
	values     []point // ring buffer
	ids        []int   // sort helper
	lastseen   time.Time
	lastupdate time.Time
	head       int
	tail       int
	size       int
	min        int64
	max        int64
	q99        int64
}

func newQuantile(size int) *quantile {
	return &quantile{
		values: make([]point, size),
		ids:    make([]int, size),
		head:   0,
		tail:   0,
		size:   size,
	}

}

func (q *quantile) add(value int64) {
	q.values[q.tail].value = value
	q.lastseen = time.Now()
	q.values[q.tail].timestamp = q.lastseen

	q.tail = (q.tail + 1) % q.size
	if q.tail == q.head {
		q.head = (q.head + 1) % q.size
	}
}

func (q *quantile) get() (mint int64, max int64, q99 int64) {
	if time.Now().Sub(q.lastupdate) > interval {
		q.caculate()
	}
	return q.min, q.max, q.q99
}

func (q *quantile) rank(left, right, rank int) {
	if rank == 0 || left >= right {
		q.q99 = q.values[q.ids[left]].value
		return
	}

	p := q.ids[left]
	v := q.values[p].value
	i := left
	j := right
	for i < j {
		for i < j && q.values[q.ids[i]].value >= v {
			j--
		}
		q.ids[i] = q.ids[j]

		for i < j && q.values[q.ids[j]].value <= v {
			i++
		}
		q.ids[j] = q.ids[i]
	}
	q.ids[i] = p

	if i < rank {
		q.rank(i+1, right, rank-i-1)
	} else if i > rank {
		q.rank(left, i-1, rank)
	} else {
		q.q99 = q.values[p].value
	}
}

func (q *quantile) caculate() {
	timestamp := time.Now()
	q.lastupdate = timestamp
	flag := false
	count := 0
	len := 0
	for i := q.head; i != q.tail; i = (i + 1) % q.size {
		if q.values[i].timestamp.Add(interval).Before(timestamp) {
			q.values[q.head].value = 0
			q.head = (q.head + 1) % q.size
			continue
		}

		q.ids[len] = i
		len++

		if !flag {
			flag = true
			if q.head >= q.tail {
				count = q.size - q.head + q.tail
			} else {
				count = q.tail - q.head
			}

			if count == 0 {
				q.min = 0
				q.max = 0
				q.q99 = 0
				return
			}
			count = int(math.Ceil(float64(count) * 0.99))

			q.max = q.values[i].value
			q.min = q.values[i].value
		}

		if q.values[i].value < q.min {
			q.min = q.values[i].value
		}
		if q.values[i].value >= q.max {
			q.max = q.values[i].value
		}
	}

	q.rank(0, len-1, count-1)
}

// QuantileGroup caculate the 99 quantile, min and max
type QuantileGroup struct {
	group      map[string]*quantile
	expiration time.Duration
	size       int
	sync.Mutex
}

// NewQuantileGroup create a QuantileGroup
func NewQuantileGroup(expiration time.Duration, size int) *QuantileGroup {
	g := &QuantileGroup{
		group:      make(map[string]*quantile),
		expiration: expiration,
		size:       size,
	}
	go g.run()
	return g
}

const interval = time.Second * 10

func (g *QuantileGroup) run() {
	ticker := time.NewTicker(g.expiration)
	for {
		<-ticker.C
		timestamp := time.Now()

		g.Lock()
		for k, v := range g.group {
			if v.lastseen.Add(g.expiration).Before(timestamp) {
				delete(g.group, k)
			}
		}
		g.Unlock()
	}
}

// Add record one point
func (g *QuantileGroup) Add(key string, value int64) {
	g.Lock()
	defer g.Unlock()

	if _, ok := g.group[key]; !ok {
		g.group[key] = newQuantile(g.size)
	}
	g.group[key].add(value)
}

// Get retrive min, max and q99
func (g *QuantileGroup) Get(key string) (mint int64, max int64, q99 int64) {
	g.Lock()
	defer g.Unlock()

	if _, ok := g.group[key]; !ok {
		return 0, 0, 0
	}
	return g.group[key].get()
}

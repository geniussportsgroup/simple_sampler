package simple_sampler

import (
	"encoding/json"
	Set "github.com/geniussportsgroup/treaps"
	"sync"
	"time"
)

type Sample struct {
	time           time.Time
	val            interface{}
	expirationTime time.Time
}

func cmpTime(s1, s2 interface{}) bool {
	t1 := s1.(*Sample).time
	t2 := s2.(*Sample).time
	return t1.Before(t2)
}

type SimpleSampler struct {
	timeIndex *Set.Treap
	valIndex  *Set.Treap
	capacity  int
	duration  time.Duration
}

// Create a simple sampler with capacity as maximum number of entries and a duration time. The entries will
// be compared with the function less
func NewSampler(capacity int, duration time.Duration, cmpVal func(s1, s2 interface{}) bool) *SimpleSampler {

	return &SimpleSampler{
		timeIndex: Set.NewTreap(cmpTime),
		valIndex: Set.NewTreap(func(i1, i2 interface{}) bool {
			return cmpVal(i1.(*Sample).val, i2.(*Sample).val)
		}),
		capacity: capacity,
		duration: duration,
	}
}

func (sampler *SimpleSampler) Size() int { return sampler.timeIndex.Size() }

// Add a new sample at time currTime with a specific val. If it already exists a sample containing
// val, then its time is updated
func (sampler *SimpleSampler) Append(currTime time.Time, val interface{}) {

	sample := &Sample{
		time:           currTime,
		val:            val,
		expirationTime: currTime.Add(sampler.duration),
	}

	ok, s := sampler.valIndex.SearchOrInsert(sample)
	if !ok { // a sample with val is already inserted?
		// yes! ==> update it with current time
		sampler.timeIndex.Remove(s) // remove from time index
		sample = s.(*Sample)
		sample.time = currTime // update time with currTime
	}

	n := sampler.valIndex.Size()
	if n > sampler.capacity {
		s := sampler.timeIndex.RemoveByPos(0) // oldest entry
		v := sampler.valIndex.Remove(s)
		if v != s {
			panic("Internal error")
		}
	}

	_ = sampler.timeIndex.Insert(sample) // it is impossible sample.time is duplicated because is after more recent
}

// Returns the maximum valid sample respect the specified duration
func (sampler *SimpleSampler) GetMax(currTime time.Time) interface{} {

	if sampler.Size() == 0 {
		return nil
	}

	newestSample := sampler.timeIndex.Max().(*Sample)
	if newestSample.expirationTime.Before(currTime) { // all entries invalid?
		// Yes ==> clean all indexes
		sampler.timeIndex.Clear()
		sampler.valIndex.Clear()
		return nil
	}

	ret := sampler.valIndex.Max().(*Sample)
	for ret.expirationTime.Before(currTime) {
		// In this case ret is not valid in period
		s := sampler.valIndex.RemoveByPos(sampler.valIndex.Size() - 1).(*Sample) // This is the max
		_ = sampler.timeIndex.Remove(s)
		ret = sampler.valIndex.Max().(*Sample)
	}

	return ret.val
}

func (sampler *SimpleSampler) OldestTime() *Sample {

	if sampler.timeIndex.Size() == 0 {
		return nil
	}

	return sampler.timeIndex.Min().(*Sample)
}

func (sampler *SimpleSampler) NewestTime() *Sample {

	if sampler.timeIndex.Size() == 0 {
		return nil
	}

	return sampler.timeIndex.Max().(*Sample)
}

func (sampler *SimpleSampler) MinimumVal() *Sample {

	if sampler.valIndex.Size() == 0 {
		return nil
	}

	return sampler.valIndex.Min().(*Sample)
}

func (sampler *SimpleSampler) MaximumVal() *Sample {

	if sampler.valIndex.Size() == 0 {
		return nil
	}

	return sampler.valIndex.Max().(*Sample)
}

func (sampler *SimpleSampler) SearchTime(time time.Time) *Sample {

	sample := Sample{
		time: time,
		val:  nil,
	}

	ret := sampler.timeIndex.Search(&sample)
	if ret == nil {
		return nil
	}

	return ret.(*Sample)
}

func (sampler *SimpleSampler) SearchVal(val interface{}) *Sample {

	sample := Sample{
		time: time.Time{},
		val:  val,
	}

	ret := sampler.valIndex.Search(&sample)
	if ret == nil {
		return nil
	}

	return ret.(*Sample)
}

type JsonSample struct {
	Time           string
	ExpirationTime string
	Val            string
}

// Helper for consulting all the samples. To be used by and endpoint
func (sampler *SimpleSampler) consultEndpoint(lock *sync.Mutex,
	convertVal func(interface{}) string) ([]byte, error) {
	lock.Lock()
	defer lock.Unlock()

	samples := make([]string, 0, sampler.Size())
	for it := Set.NewIterator(sampler.timeIndex); it.HasCurr(); it.Next() {
		sample := it.GetCurr().(*Sample)
		jsonSample := &JsonSample{
			Time:           sample.time.Format(time.RFC3339Nano),
			ExpirationTime: sample.expirationTime.Format(time.RFC3339Nano),
			Val:            convertVal(sample.val),
		}
		j, err := json.Marshal(jsonSample)
		if err != nil {
			return nil, err
		}
		samples = append(samples, string(j))
	}

	ret, err := json.Marshal(samples)

	return ret, err
}

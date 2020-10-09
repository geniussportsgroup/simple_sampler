package simple_sampler

import (
	Set "github.com/geniussportsgroup/treaps"
	"time"
)

type Sample struct {
	time time.Time
	val  interface{}
}

func cmpTime(s1, s2 interface{}) bool {
	t1 := s1.(time.Time)
	t2 := s2.(time.Time)
	return t1.Before(t2)
}

type SimpleSampler struct {
	timeIndex *Set.Treap
	valIndex  *Set.Treap
	capacity  int
	duration  time.Duration
}

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

func (sampler *SimpleSampler) append(currTime time.Time, val interface{}) {

	oldTime := currTime.Add(-sampler.duration)
	olderSamples, validSamples := sampler.timeIndex.SplitByKey(oldTime) // remove older timeIndex

	// remove of valIndex samples that are not longer valid
	for it := Set.NewIterator(olderSamples); it.HasCurr(); it.Next() {
		sampler.valIndex.Remove(it.GetCurr().(*Sample).val)
	}

	n := validSamples.Size()
	if n == sampler.capacity {
		result := sampler.valIndex.RemoveByPos(0) // deletes the minimum value
		_ = validSamples.Remove(result.(*Sample).time)
	}

	sample := &Sample{
		time: currTime,
		val:  val,
	}
	sampler.valIndex.InsertDup(sample)
	validSamples.InsertDup(sample)
	sampler.timeIndex = validSamples
}

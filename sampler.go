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

func (sampler *SimpleSampler) Append(currTime time.Time, val interface{}) {

	n := sampler.valIndex.Size()
	if n == sampler.capacity {
		s := sampler.timeIndex.RemoveByPos(0) // oldest entry
		v := sampler.valIndex.Remove(s)
		if v != s {
			panic("Internal error")
		}
	}

	sample := &Sample{
		time: currTime,
		val:  val,
	}
	sampler.valIndex.InsertDup(sample)
	sampler.timeIndex.InsertDup(sample)
}

func (sampler *SimpleSampler) Max(currTime time.Time) interface{} {

	if sampler.Size() == 0 {
		return nil
	}

	newestSample := sampler.timeIndex.Max().(*Sample)
	if newestSample.time.Add(sampler.duration).Before(currTime) {
		sampler.timeIndex.Clear()
		sampler.valIndex.Clear()
		return nil
	}

	ret := sampler.valIndex.Max().(*Sample)
	for ret.time.Add(sampler.duration).Before(currTime) {
		// In this case ret is not valid in period
		s := sampler.valIndex.RemoveByPos(sampler.valIndex.Size() - 1).(*Sample) // This is the max
		_ = sampler.timeIndex.Remove(s.time)
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

func (sampler *SimpleSampler) OldestVal() *Sample {

	if sampler.valIndex.Size() == 0 {
		return nil
	}

	return sampler.valIndex.Min().(*Sample)
}

func (sampler *SimpleSampler) NewestVal() *Sample {

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

func (sampler *SimpleSampler) MaxVal() *Sample {

	if sampler.valIndex.Size() == 0 {
		return nil
	}

	return sampler.valIndex.Max().(*Sample)
}

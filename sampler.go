package simple_sampler

import (
	"encoding/json"
	"errors"
	"fmt"
	Set "github.com/geniussportsgroup/treaps"
	"sync"
	"time"
)

const MinCapacity = 10
const MinDuration = 10 * time.Second

var TimeOfLastRequest time.Time

type Sample struct {
	time           time.Time
	val            interface{}
	expirationTime time.Time
}

type LatencySample struct {
	latency  time.Duration
	lastTime time.Time
}

func cmpTime(s1, s2 interface{}) bool {
	t1 := s1.(*Sample).time
	t2 := s2.(*Sample).time
	return t1.Before(t2)
}

func cmpLatency(s1, s2 interface{}) bool {
	return s1.(*LatencySample).latency < s2.(*LatencySample).latency
}

type SimpleSampler struct {
	timeIndex    *Set.Treap // stores the samples indexed by time
	valIndex     *Set.Treap // stores the samples indexed by value
	maxLatencies *Set.Treap // Subset of maximum latencies detected
	maxRequests  *Set.Treap // Subset of maximum number of request detected
	capacity     int
	subCapacity  int
	duration     time.Duration
}

// Create a simple sampler with capacity as maximum number of entries and a duration time. The entries will
// be compared with the function less
func NewSampler(capacity int, duration time.Duration, subSetCapacity int,
	cmpVal func(s1, s2 interface{}) bool) *SimpleSampler {

	if subSetCapacity >= capacity {
		panic(fmt.Sprintf("subset capacity %d is greater or equal than capacity %d",
			subSetCapacity, capacity))
	}

	cmpReq := func(i1, i2 interface{}) bool {
		return cmpVal(i1.(*Sample).val, i2.(*Sample).val)
	}

	ret := &SimpleSampler{
		timeIndex:   Set.NewTreap(cmpTime),
		valIndex:    Set.NewTreap(cmpReq),
		capacity:    capacity,
		subCapacity: subSetCapacity,
		duration:    duration,
	}

	if subSetCapacity != 0 {
		ret.maxLatencies = Set.NewTreap(cmpLatency)
		ret.maxRequests = Set.NewTreap(cmpReq)
	}

	return ret
}

func (sampler *SimpleSampler) updateMaxRequests(sample *Sample) {

	if sampler.subCapacity == 0 { // is subset active?
		return
	}

	if sampler.maxRequests.Size() < sampler.subCapacity { // is subset full?
		// Not ==> just insert it or update if if it is already inserted
		wasInserted, s := sampler.maxRequests.SearchOrInsert(sample)
		if !wasInserted {
			s.(*Sample).time = sample.time
			s.(*Sample).expirationTime = sample.expirationTime
		}
		return
	}

	// In this point subset is full ==> eventually we might insert this sample
	minSample := sampler.maxRequests.Min()
	if sample.val.(int) < minSample.(*Sample).val.(int) {
		return // this sample does not belong to the subset of the maximum number of requests
	}

	wasInserted, s := sampler.maxRequests.SearchOrInsert(sample)
	if !wasInserted {
		s.(*Sample).time = sample.time
		s.(*Sample).expirationTime = sample.expirationTime
	}
}

func (sampler *SimpleSampler) updateMaxLatency(latency time.Duration, currTime time.Time) {

	if sampler.subCapacity == 0 {
		return
	}

	latencySample := &LatencySample{
		latency:  latency,
		lastTime: currTime,
	}

	if sampler.maxLatencies.Size() < sampler.subCapacity { // subset full?
		// Not ==> ust insert it or update if if it is already inserted
		wasInserted, s := sampler.maxLatencies.SearchOrInsert(latencySample)
		if !wasInserted {
			s.(*Sample).time = currTime // expiration time does not matter in this case
		}
		return
	}

	// In this point subset is full ==> eventually we might insert this sample
	mimLatencySample := sampler.maxLatencies.Min()

}

// Set new values for capacity and duration
func (sampler *SimpleSampler) Set(capacity int, duration time.Duration) (err error) {

	if capacity < MinCapacity {
		err = errors.New(fmt.Sprintf("new capacity %d is less than minimum allowed %d",
			capacity, MinCapacity))
		return
	}

	if duration < MinDuration {
		err = errors.New(fmt.Sprintf("new duration %s is less than minimum allowed %s",
			FmtDuration(duration), FmtDuration(MinDuration)))
		return
	}

	for capacity < sampler.Size() {
		oldestSample := sampler.timeIndex.RemoveByPos(0)
		valSample := sampler.valIndex.Remove(oldestSample.(*Sample))
		if valSample == nil {
			err = errors.New("inconsistency has been detected. Probably a bug")
			sampler.timeIndex.Insert(oldestSample)
			return
		}
	}

	sampler.capacity = capacity

	if duration != sampler.duration {
		for it := Set.NewIterator(sampler.timeIndex); it.HasCurr(); it.Next() {
			sample := it.GetCurr().(*Sample)
			sample.expirationTime = sample.time.Add(duration)
		}
		sampler.duration = duration
	}

	return
}

func (sampler *SimpleSampler) Size() int { return sampler.timeIndex.Size() }

// Add a new sample at time currTime with a specific val. If it already exists a sample containing
// val, then its time is updated
func (sampler *SimpleSampler) Append(currTime time.Time, val interface{}) {

	sample := &Sample{val: val}

	wasInserted, s := sampler.valIndex.SearchOrInsert(sample)
	if !wasInserted { // a sample with val is already inserted?
		// yes! ==> update it with current time
		sampler.timeIndex.Remove(s) // remove from time index
		sample = s.(*Sample)        // previously built sample is discharged
	}

	sample.time = currTime // update time with currTime
	sample.expirationTime = currTime.Add(sampler.duration)

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
func (sampler *SimpleSampler) GetMax(currTime time.Time) *Sample {

	if sampler.Size() == 0 {
		return nil
	}

	newestSample := sampler.timeIndex.Max().(*Sample)
	if newestSample.expirationTime.Before(currTime) { // all entries expired?
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

	return ret
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
func (sampler *SimpleSampler) ConsultEndpoint(lock *sync.Mutex,
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

// Helper for stringficate a duration type
func FmtDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	return fmt.Sprintf("%02d:%02d", h, m)
}

// Helpers for sampling when request arrives
func (sampler *SimpleSampler) RequestArrives(requestCount *int, lock *sync.Mutex,
	notifyToScaler func(currTime time.Time)) (startTime time.Time) {

	startTime = time.Now()
	lock.Lock()
	defer lock.Unlock()
	*requestCount++
	sampler.Append(startTime, *requestCount)
	notifyToScaler(startTime) // notify load controller
	return startTime
}

// Helper for sampling when request finishes.
// The reason from which itWasSuccessful is a pointer is for handling from defer clauses which
// receive copies
func (sampler *SimpleSampler) RequestFinishes(requestCount *int, lock *sync.Mutex,
	timeOfLastRequest *time.Time, itWasSuccessful *bool) {

	endTime := time.Now()
	lock.Lock()
	defer lock.Unlock()
	*requestCount--
	sampler.Append(endTime, *requestCount)
	if !*itWasSuccessful {
		return
	}

	*timeOfLastRequest = endTime
}

// Helper for reading number of samples. It does not take lock
func (sampler *SimpleSampler) GetNumRequest(currTime time.Time) int {

	res := sampler.GetMax(currTime)
	if res == nil {
		return 0
	}
	return res.val.(int)
}

// Helper similar than above but retrieves full sample
func (sampler *SimpleSampler) GetMaxSample(currTime time.Time) (int, time.Time, time.Time) {

	sample := sampler.GetMax(currTime)
	if sample == nil {
		return 0, currTime, currTime
	}

	return sample.val.(int), sample.time, sample.expirationTime
}

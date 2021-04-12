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
	maxLatencies *Set.Treap // Subset of maximum latencies detected. Used for keep order of samples that have not expired
	maxRequests  *Set.Treap // Subset of maximum number of request detected
	capacity     int        // Maximum number of samples that I can contain
	subCapacity  int        // I store the largest maxRequest and latencies that I have seen.
	// This is the maximum number of largest maxRequest that I am able to save
	duration time.Duration // Every sample has this duration
}

// NewSampler Create a simple sampler with capacity as maximum number of entries of duration time. The entries will
// be compared with the function less. The sampler keeps two subsets with the maximums values of maxRequests and
// the maximum latencies.
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

	// subSetCapacity could be unbound. It is not the ideal because is slower, but possible
	if subSetCapacity != 0 {
		ret.maxLatencies = Set.NewTreap(cmpLatency)
		ret.maxRequests = Set.NewTreap(cmpReq)
	}

	return ret
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

// Size Return the number of possible valid samples (that have not expired) stored in the sampler. Notice
// that this number is a hint, as the samples could have expired
func (sampler *SimpleSampler) Size() int { return sampler.timeIndex.Size() }

// Append Add a new sample at time currTime with a specific val. If it already exists a sample containing
// val, then its time is updated
func (sampler *SimpleSampler) Append(currTime time.Time, val interface{}) {

	sample := &Sample{val: val} // Create a new Sample ptr where putting val

	wasInserted, s := sampler.valIndex.SearchOrInsert(sample) // test the sample already exists
	if !wasInserted {                                         // a sample with val is already inserted?
		// yes! ==> we just update it with current time
		sampler.timeIndex.Remove(s) // remove from time index. It could fail if there is not sample store in timeIndex
		sample = s.(*Sample)        // previously allocated sample is discharged
	}

	sample.time = currTime // update time with currTime
	sample.expirationTime = currTime.Add(sampler.duration)

	n := sampler.valIndex.Size()
	if n > sampler.capacity { // sampler full?
		// Yes ==> we must delete the less pertinent entry so that we guarantee the set is still bounded
		s := sampler.timeIndex.RemoveByPos(0) // remove the oldest entry
		v := sampler.valIndex.Remove(s)       // now I remove
		if v != s {
			panic("Internal error")
		}
	}

	_ = sampler.timeIndex.Insert(sample) // and now we put the new and fresh value in our time index

	sampler.updateMaxRequests(sample) // from here we update val index with this helper
}

// Helper to be called when a new sample is inserted. The goal is
func (sampler *SimpleSampler) updateMaxRequests(sample *Sample) {

	if sampler.subCapacity == 0 { // is subset active?
		return
	}

	if sampler.maxRequests.Size() < sampler.subCapacity { // is subset full?
		// Not ==> just insert it or update if if it is already inserted
		_, _ = sampler.maxRequests.SearchOrInsert(sample)
		return
	}

	// In this point subset is full ==> eventually we might have to insert this sample
	minSample := sampler.maxRequests.Min()
	if sample.val.(int) < minSample.(*Sample).val.(int) { // sample.val < minimum store?
		return // this sample does not belong to the subset of the maximum number of requests
	}

	// In this case, this sample must belong to the set ==> we insert it
	wasInserted, _ := sampler.maxRequests.SearchOrInsert(sample)
	if wasInserted {
		// sample val could already belong to the set. But not in this case, because it was inserted. Now,
		// because the set is full, we remove the minimum
		sampler.maxRequests.RemoveByPos(0) // remove the minimum
	}
}

// GetMax Returns the maximum valid sample. By valid we mean that the sample has not expired
func (sampler *SimpleSampler) GetMax(currTime time.Time) *Sample {

	if sampler.Size() == 0 {
		return nil
	}

	moreRecentSample := sampler.timeIndex.Max().(*Sample)
	if moreRecentSample.expirationTime.Before(currTime) { // all entries expired?
		// if moreRecentSample expired ==> all the remaining too ==> we delete them
		sampler.timeIndex.Clear()
		sampler.valIndex.Clear()
		return nil
	}

	// Now we proceed to examine through the values starting by the maximum until finding the first one that
	// is valid; i.e. that has not expired yet. Because moreRecentSample is valid, it is sure that we will find
	// a valid sample. Notice that we perform this search through valIndex instead of timeIndex
	currMaxValueSample := sampler.valIndex.Max().(*Sample)
	for currMaxValueSample.expirationTime.Before(currTime) { // currMaxValueSample expired
		// yes ==> we remove from out indexes
		s := sampler.valIndex.RemoveByPos(sampler.valIndex.Size() - 1).(*Sample) // This is the max
		_ = sampler.timeIndex.Remove(s)
		currMaxValueSample = sampler.valIndex.Max().(*Sample) // get a new one for being tested
	}

	return currMaxValueSample
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
	Time           time.Time
	ExpirationTime time.Time
	Val            int
}

type JsonLatencySample struct {
	Latency  int // msec
	LastTime time.Time
}

type JsonMaximums struct {
	MaxRequests  []*JsonSample
	MaxLatencies []*JsonLatencySample
}

// Helper
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
			s.(*LatencySample).lastTime = currTime // update
		}
		return
	}

	// In this point subset is full ==> eventually we might insert this sample
	minLatencySample := sampler.maxLatencies.Min()
	if latency < minLatencySample.(*LatencySample).latency {
		return // this sample does not belong to the subset of the maximum number of requests
	}

	wasInserted, s := sampler.maxLatencies.SearchOrInsert(latencySample)
	if !wasInserted {
		s.(*LatencySample).lastTime = currTime // update
	} else {
		sampler.maxLatencies.RemoveByPos(0) // remove minimum latency present in the set
	}
}

// ConsultEndpoint Helper for consulting all the samples. To be used by and endpoint
func (sampler *SimpleSampler) ConsultEndpoint(lock *sync.Mutex) ([]byte, error) {

	lock.Lock()
	defer lock.Unlock()

	samples := make([]*JsonSample, 0, sampler.Size())
	for it := Set.NewIterator(sampler.timeIndex); it.HasCurr(); it.Next() {
		sample := it.GetCurr().(*Sample)
		samples = append(samples, &JsonSample{
			Time:           sample.time,
			ExpirationTime: sample.expirationTime,
			Val:            sample.val.(int),
		})
	}

	ret, err := json.Marshal(samples)

	return ret, err
}

// ConsultMaximumsEndpoint Helper for consulting maximums values (request and latencies)
func (sampler *SimpleSampler) ConsultMaximumsEndpoint(lock *sync.Mutex) ([]byte, error) {

	lock.Lock()
	defer lock.Unlock()

	maxSamples := make([]*JsonSample, 0, sampler.maxRequests.Size())
	for it := Set.NewIterator(sampler.maxRequests); it.HasCurr(); it.Next() {
		sample := it.GetCurr().(*Sample)
		jsonSample := &JsonSample{
			Time:           sample.time,
			ExpirationTime: sample.expirationTime,
			Val:            sample.val.(int),
		}
		maxSamples = append(maxSamples, jsonSample)
	}

	latencySamples := make([]*JsonLatencySample, 0, sampler.maxLatencies.Size())
	for it := Set.NewIterator(sampler.maxLatencies); it.HasCurr(); it.Next() {
		sample := it.GetCurr().(*LatencySample)
		milliseconds := sample.latency.Nanoseconds() / 1e6
		jsonSample := &JsonLatencySample{
			Latency:  int(milliseconds),
			LastTime: sample.lastTime,
		}
		latencySamples = append(latencySamples, jsonSample)
	}

	ret := &JsonMaximums{
		MaxRequests:  maxSamples,
		MaxLatencies: latencySamples,
	}

	return json.MarshalIndent(ret, "", "  ")
}

// FmtDuration Helper for stringficate a duration type
func FmtDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	return fmt.Sprintf("%02d:%02d", h, m)
}

// RequestArrives Helper for sampling when request arrives. When a new request is detected, the counter must
// be updated. Such a counter (requestCount) is passed to the sampler through this routine. The lock assures
// that operation is atomic and keeps safe the sampler state. notifyToScaler is a pointer to a function with
// should communicate with the scaler.
// The routine returns the arrivalTime to the sample whose value will be stored as timestamp along with its
// expiration time
func (sampler *SimpleSampler) RequestArrives(requestCount *int, lock *sync.Mutex,
	notifyToScaler func(currTime time.Time)) (arrivalTime time.Time) {

	arrivalTime = time.Now()
	lock.Lock()
	defer lock.Unlock()
	TimeOfLastRequest = arrivalTime
	*requestCount++
	sampler.Append(arrivalTime, *requestCount)
	notifyToScaler(arrivalTime) // notify load controller
	return arrivalTime
}

// RequestFinishes Analogously, when a request finishes, the requestCount must decrease and update it to the
// sampler. Sometimes, the request could fail. To indicate that, use the itWasSuccessful bool with must set
// to true once it is sure that the request succeeded
func (sampler *SimpleSampler) RequestFinishes(requestCount *int, lock *sync.Mutex,
	arrivalTime time.Time, itWasSuccessful *bool) time.Duration {

	endTime := time.Now()
	latency := endTime.Sub(arrivalTime)
	lock.Lock()
	defer lock.Unlock()
	*requestCount--
	sampler.Append(endTime, *requestCount)
	if !*itWasSuccessful {
		return 0
	}

	sampler.updateMaxLatency(latency, arrivalTime)
	return latency
}

// GetNumRequest Helper for reading number of samples. It does not take lock
func (sampler *SimpleSampler) GetNumRequest(currTime time.Time) int {

	res := sampler.GetMax(currTime)
	if res == nil {
		return 0
	}
	return res.val.(int)
}

// GetMaxSample Helper similar than above but retrieves full sample
func (sampler *SimpleSampler) GetMaxSample(currTime time.Time) (int, time.Time, time.Time) {

	sample := sampler.GetMax(currTime)
	if sample == nil {
		return 0, currTime, currTime
	}

	return sample.val.(int), sample.time, sample.expirationTime
}

package simple_sampler

import (
	"fmt"
	Set "github.com/geniussportsgroup/treaps"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSimpleSampler_append(t *testing.T) {

	const RandBase = 300
	const N = 100
	sampler := NewSampler(100, time.Minute, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	max := -1
	for i := 0; i < N; i++ {
		val := rand.Intn(RandBase)
		max = int(math.Max(float64(val), float64(max)))
		sampler.Append(time.Now(), val)
	}

	// 990 mi

	maximum := sampler.MaximumVal()
	timeOfMax := maximum.time
	maxVal := maximum.val.(int)

	fmt.Println(timeOfMax.String())
	fmt.Println("max =", maxVal)

	assert.Equal(t, max, sampler.GetMax(time.Now()))

	oldestSample := sampler.OldestTime()
	assert.Equal(t, oldestSample, sampler.SearchTime(oldestSample.time))

	n := sampler.Size()
	for i, k := n-1, 1; i <= N; i, k = i+1, k+1 { // assures to fill all the sampler
		sampler.Append(time.Now(), max+k) // assure not duplicates
	}
	// at this point oldestSample should have been evicted

	assert.Nil(t, sampler.SearchTime(oldestSample.time))
}

func TestSimpleSampler_Correctness(t *testing.T) {

	const N = 200
	const BaseValue = 300
	const Period = time.Minute

	sampler := NewSampler(N, Period, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	for i := 0; i < N/2; i++ {
		sampler.Append(time.Now(), BaseValue+i)
	}

	assert.Equal(t, N/2, sampler.Size())
	assert.Equal(t, BaseValue+N/2-1, sampler.GetMax(time.Now()))

	time.Sleep(Period) // after all the entries should have expired

	assert.Nil(t, sampler.GetMax(time.Now()))
	assert.Equal(t, 0, sampler.Size())

	for i := 0; i < N/2; i++ {
		sampler.Append(time.Now(), BaseValue+i)
	}
	m := sampler.GetMax(time.Now())
	fmt.Println("max=", m)
	time.Sleep(28 * time.Second)
	for i := 0; i < N/2; i++ {
		sampler.Append(time.Now(), i)
	}
	// In this moment the sample contains exactly N entries (its capacity)

	assert.Equal(t, N, sampler.Size())
	m = sampler.GetMax(time.Now())
	fmt.Println("max=", m)
	assert.Equal(t, BaseValue+N/2-1, m)

	// Now we put a new entry for testing if the oldest are evicted
	oldestSample := sampler.OldestTime()
	assert.Equal(t, oldestSample, sampler.SearchTime(oldestSample.time))
	assert.Equal(t, oldestSample, sampler.SearchVal(oldestSample.val))

	assert.Nil(t, sampler.SearchVal(N+1))
	sampler.Append(time.Now(), N+1) // this insertion should evict oldestSample
	assert.Nil(t, sampler.SearchVal(oldestSample.val))
	assert.Nil(t, sampler.SearchTime(oldestSample.time))

	// now we manage for making the maximum invalid in period
	time.Sleep(33 * time.Second)
	// Elapsed this time, the first N/2 entries should have been evicted
	m = sampler.GetMax(time.Now())
	assert.Equal(t, N+1, m)
	fmt.Println("max=", m)
}

func TestSimpleSampler_CornerCases(t *testing.T) {

	const N = 200
	const BaseValue = 300
	const Period = time.Minute

	beginTime := time.Now()

	sampler := NewSampler(N, Period, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	for i := 0; i < N/2; i++ {
		sampler.Append(time.Now(), BaseValue+i)
	}

	assert.Panics(t, func() {
		sampler.Append(beginTime, 10)
	})

	secondTimeSample := sampler.timeIndex.Choose(1)
	assert.Panics(t, func() {
		sampler.Append(secondTimeSample.(*Sample).time, 10)
	})

}

func TestSimpleSampler_SearchFunctions(t *testing.T) {
	const N = 200
	const BaseValue = 300
	const Period = time.Minute
	sampler := NewSampler(N, Period, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	beginTime := time.Now()

	for i := 0; i < N/2; i++ {
		sampler.Append(time.Now(), BaseValue+i)
	}

	for it := Set.NewIterator(sampler.timeIndex); it.HasCurr(); it.Next() {
		assert.NotNil(t, sampler.SearchTime(it.GetCurr().(*Sample).time))
		assert.NotNil(t, sampler.SearchVal(it.GetCurr().(*Sample).val))
	}

	assert.Nil(t, sampler.SearchTime(beginTime))
	assert.Nil(t, sampler.SearchVal(N))
}

func TestSimpleSampler_GetMax(t *testing.T) {
	const N = 200
	const BaseValue = 300
	const Period = time.Minute
	sampler := NewSampler(N, Period, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	assert.Nil(t, sampler.GetMax(time.Now()))

	for i := 0; i < N/2; i++ {
		sampler.Append(time.Now(), BaseValue+i)
	}

	assert.Equal(t, BaseValue+N/2-1, sampler.GetMax(time.Now()))
}

func TestSimpleSampler_Observers(t *testing.T) {
	const N = 200
	const BaseValue = 300
	const Period = time.Minute
	sampler := NewSampler(N, Period, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	assert.Nil(t, sampler.MaximumVal())
	assert.Nil(t, sampler.MinimumVal())
	assert.Nil(t, sampler.GetMax(time.Now()))
	assert.Nil(t, sampler.OldestTime())
	for i := 0; i < N/2; i++ {
		sampler.Append(time.Now(), BaseValue+i)
	}
	assert.Equal(t, BaseValue, sampler.MinimumVal().val)
	assert.Equal(t, BaseValue+N/2-1, sampler.MaximumVal().val)
}

func TestSimpleSampler_consultEndpoint(t *testing.T) {
	lock := new(sync.Mutex)

	const N = 200
	const BaseValue = 300
	const Period = time.Minute
	sampler := NewSampler(N, Period, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	for i := 0; i < N; i++ {
		sampler.Append(time.Now(), 100+rand.Intn(BaseValue))
	}

	b, _ := sampler.consultEndpoint(lock, func(i interface{}) string {
		return strconv.Itoa(i.(int))
	})

	fmt.Println(string(b))
}

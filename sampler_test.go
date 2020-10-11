package simple_sampler

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
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

	maximum := sampler.MaxVal()
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

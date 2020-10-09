package simple_sampler

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestSimpleSampler_append(t *testing.T) {

	sampler := NewSampler(100, time.Minute, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	firstTime := time.Now()
	sampler.Append(firstTime, rand.Intn(300))
	for i := 1; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		sampler.Append(time.Now(), rand.Intn(300))
	}

	maximum := sampler.MaxVal()
	timeOfMax := maximum.time
	maxVal := maximum.val.(int)

	fmt.Println(timeOfMax.String())
	fmt.Println("max =", maxVal)

	fmt.Println("Oldest entry =", sampler.timeIndex.Min())

	time.Sleep(time.Second)

	sampler.Append(time.Now(), rand.Intn(300))

	assert.Nil(t, sampler.SearchTime(firstTime), "first inserted entry should be evicted")

	firstTime = sampler.OldestTime().time

	assert.NotNil(t, sampler.SearchTime(firstTime))

	sampler.Append(time.Now(), 400)

	assert.Nil(t, sampler.SearchTime(firstTime), "oldest inserted entry should be evicted")

	assert.Equal(t, 400, sampler.MaxVal().val)
	assert.Equal(t, 400, sampler.NewestTime().val)
	assert.Equal(t, 100, sampler.Size())

	// now we put other entries for dragging the biggest

	for i := 0; i < 10; i++ {
		sampler.Append(time.Now(), rand.Intn(300))
	}

	m := sampler.Max(time.Now())
	fmt.Println(m)
}

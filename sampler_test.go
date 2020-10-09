package simple_sampler

import (
	"math/rand"
	"testing"
	"time"
)

func TestSimpleSampler_append(t *testing.T) {

	sampler := NewSampler(100, time.Minute, func(s1, s2 interface{}) bool {
		return s1.(int) < s2.(int)
	})

	for i := 0; i < 100; i++ {
		sampler.append(time.Now(), rand.Intn(300))
	}
}

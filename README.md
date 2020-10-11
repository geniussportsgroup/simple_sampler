# simple_sampler

In this context, a sampler can be seen as a finite time series. 

A simple sample is a finite time series. That is, a finite sequence of pairs containing the time when the sample was taken and the sampled value.

The goal of this sampler is to quickly compute the highest value stored in the whole sequence, and it is valid respect to a specified time to live or duration, which is given at sampler creation time.

## Sampler creation

Use the function \`NewSampler(capacity int, duration time.Duration, cmpVal func(s1, s2 interface{}) bool)\` which returns a new sampler.

The \`capacity\` parameter determines the maximum number of samples that could be stored.

The \`duration\` parameter is a period where the sample is considered valid. Given any sample, once the duration has elapsed, the sample is deleted automatically.

\`cmpVal()\` is comparator function for the sampled values. The function must implement a less than semantic.

A sampler can contain values of any type, but this type must be constant through the entire sequence. That means, for example, that you cannot have a sampler containing integer and floats. The \`cmpVal()\` must handle the type and perform the necessary castings.

## Sample insertion

The method \`Append(time, value)\` is intended for inserting a new \`value\` sampled at any specific time. \`time\` must be more recent that the last previously inserted samples.

If the sampler is full, that is, the number of stored samples is equal to sampler's capacity, then the oldest sample is evicted.

The method panics if \`time\` is before the last inserted sample (time must be monotonic increasing).

## Getting the highest value

Use the method \`GetMax(currTime time.Time)\` which returns the highest stored value valid respect to the duration. If all the stored samples are already invalid respect to \`currTime\`, then these ones are deleted, and the sampler becomes empty.

\`currTime\` must be recent that the last inserted sample; otherwise the method panics.

If not sample is found, then the method returns \`nil\`.

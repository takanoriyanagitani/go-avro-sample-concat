package samples2avro

import (
	"iter"
)

type SampleNames iter.Seq[string]

type NumberOfSamplesPerName uint16

type OriginalMaps iter.Seq2[map[string]any, error]

type SampleMaps iter.Seq2[map[string]any, error]

type Sampler func(OriginalMaps) SampleMaps

func (n NumberOfSamplesPerName) ToSampler() Sampler {
	return func(original OriginalMaps) SampleMaps {
		return func(yield func(map[string]any, error) bool) {
			var cnt uint16
			for row, e := range original {
				if uint16(n) <= cnt {
					return
				}

				if !yield(row, e) {
					return
				}

				cnt += 1
			}
		}
	}
}

package source

import (
	"bufio"
	"context"
	"log"
	"os"

	sc "github.com/takanoriyanagitani/go-avro-sample-concat"
	. "github.com/takanoriyanagitani/go-avro-sample-concat/util"
)

type FilenameToMaps func(string) IO[sc.OriginalMaps]

func (s FilenameToMaps) ToSamples(
	names sc.SampleNames,
	sampler sc.Sampler,
) IO[sc.SampleMaps] {
	return func(ctx context.Context) (sc.SampleMaps, error) {
		return func(yield func(map[string]any, error) bool) {
			for name := range names {
				select {
				case <-ctx.Done():
					yield(nil, ctx.Err())
					return
				default:
				}

				originals, e := s(name)(ctx)
				if nil != e {
					yield(nil, e)
					return
				}

				var samples sc.SampleMaps = sampler(originals)
				for sample, e := range samples {
					if !yield(sample, e) {
						return
					}
				}
			}
		}, nil
	}
}

func StdinToNames() sc.SampleNames {
	return func(yield func(string) bool) {
		var s *bufio.Scanner = bufio.NewScanner(os.Stdin)

		for s.Scan() {
			var filename string = s.Text()
			if !yield(filename) {
				return
			}
		}

		var e error = s.Err()
		if nil != e {
			log.Printf("error while getting filenames from stdin: %v\n", e)
		}
	}
}

func (s FilenameToMaps) StdinToNamesToSamples(
	sampler sc.Sampler,
) IO[sc.SampleMaps] {
	return s.ToSamples(StdinToNames(), sampler)
}

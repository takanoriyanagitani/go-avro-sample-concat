package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	sc "github.com/takanoriyanagitani/go-avro-sample-concat"
	dh "github.com/takanoriyanagitani/go-avro-sample-concat/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-sample-concat/avro/enc/hamba"
	sf "github.com/takanoriyanagitani/go-avro-sample-concat/avro/source/fs"
	. "github.com/takanoriyanagitani/go-avro-sample-concat/util"
)

func envVarByKey(key string) IO[string] {
	return func(_ context.Context) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	}
}

func filenameToStringLimited(limit int64) func(string) IO[string] {
	return func(filename string) IO[string] {
		return func(_ context.Context) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()

			limited := &io.LimitedReader{
				R: f,
				N: limit,
			}

			var bldr strings.Builder

			_, e = io.Copy(&bldr, limited)
			return bldr.String(), e
		}
	}
}

const SchemaSizeLimitDefault int64 = 1048576

var schemaName IO[string] = envVarByKey("ENV_SCHEMA_FILENAME")

var schemaContent IO[string] = Bind(
	schemaName,
	filenameToStringLimited(SchemaSizeLimitDefault),
)

var blobSizeMax IO[int] = Bind(
	envVarByKey("ENV_BLOB_SIZE_MAX"),
	Lift(strconv.Atoi),
).Or(Of(sc.BlobSizeMaxDefault))

var decodeConf IO[sc.DecodeConfig] = Bind(
	blobSizeMax,
	Lift(func(i int) (sc.DecodeConfig, error) {
		return sc.DecodeConfig{BlobSizeMax: i}, nil
	}),
)

var filename2maps IO[sf.FilenameToMaps] = Bind(
	decodeConf,
	Lift(func(dc sc.DecodeConfig) (sf.FilenameToMaps, error) {
		return func(filename string) IO[sc.OriginalMaps] {
			return func(_ context.Context) (sc.OriginalMaps, error) {
				var i iter.Seq2[map[string]any, error] = dh.FilenameToMaps(
					filename,
					dc,
				)
				return sc.OriginalMaps(i), nil
			}
		}, nil
	}),
)

var numberOfSamplesInt IO[int] = Bind(
	envVarByKey("ENV_NUM_OF_SAMPLES_PER_FILE"),
	Lift(strconv.Atoi),
)

var sampler IO[sc.Sampler] = Bind(
	numberOfSamplesInt,
	Lift(func(i int) (sc.Sampler, error) {
		return sc.NumberOfSamplesPerName(i).ToSampler(), nil
	}),
)

var stdin2names2samples IO[sc.SampleMaps] = Bind(
	filename2maps,
	func(name2maps sf.FilenameToMaps) IO[sc.SampleMaps] {
		return Bind(
			sampler,
			name2maps.StdinToNamesToSamples,
		)
	},
)

func samples2stdout(samples iter.Seq2[map[string]any, error]) IO[Void] {
	return Bind(
		schemaContent,
		func(schema string) IO[Void] {
			return Bind(
				Of(samples),
				eh.SchemaToMapsToStdoutDefault(schema),
			)
		},
	)
}

func sampleMapsToStdout(samples sc.SampleMaps) IO[Void] {
	return samples2stdout(iter.Seq2[map[string]any, error](samples))
}

var stdin2names2samples2avro2stdout IO[Void] = Bind(
	stdin2names2samples,
	sampleMapsToStdout,
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2names2samples2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}

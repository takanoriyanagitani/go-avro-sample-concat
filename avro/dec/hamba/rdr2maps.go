package dec

import (
	"bufio"
	"fmt"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"
	sc "github.com/takanoriyanagitani/go-avro-sample-concat"
	. "github.com/takanoriyanagitani/go-avro-sample-concat/util"
)

func ReaderToMapsHamba(
	rdr io.Reader,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}

		var br io.Reader = bufio.NewReader(rdr)

		dec, err := ho.NewDecoder(br, opts...)
		if nil != err {
			yield(buf, err)

			return
		}

		for dec.HasNext() {
			clear(buf)

			err = dec.Decode(&buf)
			if nil != err {
				err = fmt.Errorf("%w: unable to decode", err)
			}
			if !yield(buf, err) {
				return
			}
		}
	}
}

func ConfigToOpts(cfg sc.DecodeConfig) []ho.DecoderFunc {
	var blobSizeMax int = cfg.BlobSizeMax

	var hcfg ha.Config
	hcfg.MaxByteSliceSize = blobSizeMax

	var hapi ha.API = hcfg.Freeze()

	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMaps(
	rdr io.Reader,
	cfg sc.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	var opts []ho.DecoderFunc = ConfigToOpts(cfg)

	return ReaderToMapsHamba(
		rdr,
		opts...,
	)
}

func FilenameToMaps(
	filename string,
	cfg sc.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		f, e := os.Open(filename)
		if nil != e {
			yield(nil, e)
			return
		}
		defer f.Close()

		var br io.Reader = bufio.NewReader(f)
		var i iter.Seq2[map[string]any, error] = ReaderToMaps(
			br,
			cfg,
		)

		for row, e := range i {
			if !yield(row, e) {
				return
			}
		}
	}
}

func StdinToMaps(
	cfg sc.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMaps(os.Stdin, cfg)
}

var StdinToMapsDefault IO[iter.Seq2[map[string]any, error]] = OfFn(
	func() iter.Seq2[map[string]any, error] {
		return StdinToMaps(sc.DecodeConfigDefault)
	},
)

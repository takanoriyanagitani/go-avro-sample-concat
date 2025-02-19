package util

import (
	sc "github.com/takanoriyanagitani/go-avro-sample-concat"
)

func ComposeErr[T, U, V any](
	f func(T) (U, error),
	g func(U) (V, error),
) func(T) (V, error) {
	return sc.ComposeErr(f, g)
}

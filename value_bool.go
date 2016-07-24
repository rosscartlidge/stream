package stream

import (
	"strconv"
)

type Bool bool

func (b Bool) Int() (int64, error) {
	if !b {
		return 0, nil
	}
	return 1, nil
}

func (b Bool) Float() (float64, error) {
	if !b {
		return 0.0, nil
	}
	return 1.0, nil
}

func (b Bool) String() (string, error) {
	return strconv.FormatBool(bool(b)), nil
}

func (b Bool) Bool() (bool, error) {
	return bool(b), nil
}

func (b Bool) Stream() (Stream, error) {
	m := Record{"Bool": b}
	returned := false
	n := func() (Record, error) {
		if returned {
			return nil, EOS
		}
		returned = true
		return m, nil
	}
	return Stream(n), nil
}

func GetBool(r Record, f string) bool {
	v, ok := r[f]
	if !ok {
		v = Empty{}
	}
	b, _ := v.Bool()
	return b
}

package stream

import (
	"strconv"
)

type Float float64

func (f Float) Int() (int64, error) {
	return int64(f), nil
}

func (f Float) Float() (float64, error) {
	return float64(f), nil
}

func (f Float) String() (string, error) {
	return strconv.FormatFloat(float64(f), 'g', -1, 64), nil
}

func (f Float) Bool() (bool, error) {
	return f != 0.0, nil
}

func (f Float) Stream() (Stream, error) {
	m := Record{"Float": f}
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

func GetFloat(r Record, f string) float64 {
	v, ok := r[f]
	if !ok {
		v = Empty{}
	}
	flt, _ := v.Float()
	return flt
}

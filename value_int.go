package stream

import (
	"strconv"
)

type Int int64

func (i Int) Int() (int64, error) {
	return int64(i), nil
}

func (i Int) Float() (float64, error) {
	return float64(i), nil
}

func (i Int) String() (string, error) {
	return strconv.FormatInt(int64(i), 10), nil
}

func (i Int) Bool() (bool, error) {
	return i != 0, nil
}

func (i Int) Stream() (Stream, error) {
	m := Record{"Int": i}
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

func GetInt(r Record, f string) int64 {
	v, ok := r[f]
	if !ok {
		v = Empty{}
	}
	i, _ := v.Int()
	return i
}

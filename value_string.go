package stream

import (
	"strconv"
)

type String string

func (s String) Int() (int64, error) {
	i, err := strconv.ParseInt(string(s), 10, 64)
	return i, err
}

func (s String) Float() (float64, error) {
	f, err := strconv.ParseFloat(string(s), 64)
	return f, err
}

func (s String) String() (string, error) {
	return string(s), nil
}

func (s String) Bool() (bool, error) {
	return s != "", nil
}

func (s String) Stream() (Stream, error) {
	m := Record{"String": s}
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

func GetString(r Record, f string) string {
	v, ok := r[f]
	if !ok {
		v = Empty{}
	}
	s, _ := v.String()
	return s
}

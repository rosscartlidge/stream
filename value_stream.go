package stream

import (
	"encoding/json"
	"fmt"
)

func (s Stream) Int() (int64, error) {
	var d int64
	return d, fmt.Errorf("Value is Stream not Int")
}

func (s Stream) Float() (float64, error) {
	var d float64
	return d, fmt.Errorf("Value is Stream not Float")
}

func (s Stream) Bool() (bool, error) {
	var d bool
	return d, fmt.Errorf("Value is Stream not Bool")
}

func (s Stream) String() (string, error) {
	var d string
	return d, fmt.Errorf("Value is Stream not String")
}

func (s Stream) Stream() (Stream, error) {
	return s, nil
}

func (s Stream) MarshalJSON() ([]byte, error) {
	return json.Marshal(StreamToRecords(s))
}

func GetStream(r Record, f string) Stream {
	v, ok := r[f]
	if !ok {
		v = Empty{}
	}
	s, _ := v.Stream()
	return s
}

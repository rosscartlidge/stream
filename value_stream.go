package stream

import (
	"bytes"
	"encoding/json"
	"encoding/gob"
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

func (s Stream) GobEncode() ([]byte, error) {
	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	err := e.Encode(StreamToRecords(s))
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (s *Stream) GobDecode(bs []byte)  error {
	d := gob.NewDecoder(bytes.NewBuffer(bs))
	var rs []Record
	err := d.Decode(&rs)
	if err != nil {
		return err
	}
	*s = RecordsToStream(rs)
	return nil
}

func GetStream(r Record, f string) Stream {
	v, ok := r[f]
	if !ok {
		v = Empty{}
	}
	s, _ := v.Stream()
	return s
}

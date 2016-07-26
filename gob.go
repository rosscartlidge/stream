package stream

import (
	"encoding/gob"
	"fmt"
	"io"
)

func init() {
	gob.Register(Int(0))
	gob.Register(String(""))
	gob.Register(Bool(false))
	gob.Register(Float(0.0))
	gob.Register(Stream(nil))
}
func StreamToGob(o io.Writer, i Stream) {
	e := gob.NewEncoder(o)
	for {
		r, err := i()
		if err != nil {
			break
		}
		err = e.Encode(r)
		if err != nil {
			fmt.Println("error:", err)
		}
	}
}

func GobToStream(i io.Reader) Stream {
	d := gob.NewDecoder(i)
	return func() (Record, error) {
		var r Record
		if err := d.Decode(&r); err != nil {
			return nil, EOS
		}
		return r, nil
	}
}

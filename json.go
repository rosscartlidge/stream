package stream

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func StreamToJSON(o io.Writer, i Stream) {
	for {
		r, err := i()
		if err != nil {
			break
		}
		b, err := json.Marshal(r)
		if err != nil {
			fmt.Println("error:", err)
		}
		os.Stdout.Write(b)
	}
}

func ConvertToRecord(m map[string]interface{}) Record {
	r := Record{}
	for f, x := range m {
		switch v := x.(type) {
		case int64:
			r[f] = Int(v)
		case float64:
			r[f] = Float(v)
		case string:
			r[f] = String(v)
		case bool:
			r[f] = Bool(v)
		case []interface{}:
			var rs []Record
			for _, x := range v {
				m := x.(map[string]interface{})
				rs = append(rs, ConvertToRecord(m))
			}
			r[f] = RecordsToStream(rs)
		}
	}
	return r
}

func JSONToStream(i io.Reader) Stream {
	d := json.NewDecoder(i)
	return func() (Record, error) {
		var m map[string]interface{}
		if err := d.Decode(&m); err != nil {
			return nil, EOS
		}
		r := ConvertToRecord(m)
		return r, nil
	}
}

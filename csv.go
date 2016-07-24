package stream

import (
	"encoding/csv"
	"io"
	"sort"
)

func ToCSV(o io.Writer) Filter {
	return func(i Stream) Stream {
		return func() (Record, error) {
			var records []Record
			fm := make(map[string]bool)
			for {
				r, err := i()
				if err != nil {
					break
				}
				for f := range r {
					fm[f] = true
				}
				records = append(records, r)
			}
			var fs []string
			for f := range fm {
				fs = append(fs, f)
			}
			sort.Sort(sort.StringSlice(fs))
			w := csv.NewWriter(o)
			w.Write(fs)
			for _, r := range records {
				var l []string
				for _, f := range fs {
					l = append(l, GetString(r, f))
				}
				w.Write(l)
			}
			w.Flush()
			return nil, EOS
		}
	}
}

func StreamToCSV(o io.Writer, i Stream) {
	var header []string
	w := csv.NewWriter(o)
	for {
		r, err := i()
		if err != nil {
			break
		}
		if header == nil {
			for f := range r {
				header = append(header, f)
			}
			sort.Sort(sort.StringSlice(header))
			w.Write(header)
		}
		var l []string
		for _, f := range header {
			l = append(l, GetString(r, f))
		}
		w.Write(l)
	}
	w.Flush()
}

func CSVToStream(i io.Reader) Stream {
	r := csv.NewReader(i)
	var header []string
	return func() (Record, error) {
		if header == nil {
			var err error
			header, err = r.Read()
			if err != nil {
				return nil, EOS
			}
		}
		data, err := r.Read()
		if err != nil {
			return nil, EOS
		}
		r := make(Record)
		for i, f := range header {
			r[f] = String(data[i])
		}
		return r, nil
	}
}

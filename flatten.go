package stream

import (
	"context"
	"strings"
)

func cross(columns [][]Record) []Record {
	if len(columns) == 0 {
		return nil
	}
	if len(columns) == 1 {
		return columns[0]
	}
	var rs []Record
	for _, lr := range cross(columns[1:]) {
		for _, rr := range columns[0] {
			r := make(Record)
			for f := range rr {
				r[f] = rr[f]
			}
			for f := range lr {
				r[f] = lr[f]
			}
			rs = append(rs, r)
		}
	}
	return rs
}

func Flatten(r Record, sep string) []Record {
	var columns [][]Record
	var fs []string
	for f := range r {
		if s, ok := r[f].(Stream); ok {
			var rs []Record
			for {
				if r, err := s(); err == nil {
					if f != "" {
						er := make(Record, len(r))
						for fi := range r {
							ef := []string{f}
							if fi != "" {
								ef = append(ef, fi)
							}
							er[strings.Join(ef, sep)] = r[fi]
						}
						rs = append(rs, Flatten(er, sep)...)
					} else {
						rs = append(rs, Flatten(r, sep)...)
					}
				} else {
					break
				}
			}
			columns = append(columns, rs)
		} else {
			fs = append(fs, f)
		}
	}
	if len(columns) == 0 {
		return []Record{r}
	}
	crs := cross(columns)
	if len(fs) == 0 {
		return crs
	}
	for _, cr := range crs {
		for _, f := range fs {
			cr[f] = r[f]
		}
	}
	return crs
}

func Expand() Filter {
	return func(i Stream) Stream {
		var expanded Stream
		return func() (Record, error) {
			for {
				if expanded == nil {
					if r, err := i(); err != nil {
						return nil, EOS
					} else {
						expanded = RecordsToStream(Flatten(r, "."))
					}
				}
				if r, err := expanded(); err == nil {
					return r, nil
				}
				expanded = nil
			}
		}
	}
}

func GofilterExpand() Gofilter {
	return func(ctx context.Context, out, in chan Record) {
		for r := range in {
			for _, er := range Flatten(r, ".") {
				out <- er
			}
		}
	}
}

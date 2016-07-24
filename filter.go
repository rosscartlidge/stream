package stream

import (
	"sort"
)

type RecordSort struct {
	records []Record
	less    func(r1, r2 Record) bool
}

func (rs RecordSort) Len() int {
	return len(rs.records)
}

func (rs RecordSort) Less(i, j int) bool {
	return rs.less(rs.records[i], rs.records[j])
}

func (rs RecordSort) Swap(i, j int) {
	rs.records[i], rs.records[j] = rs.records[j], rs.records[i]
}

func Sort(less func(r1, r2 Record) bool) Filter {
	return func(i Stream) Stream {
		sortable := RecordSort{less: less}
		var rs Stream
		return func() (Record, error) {
			if rs == nil {
				for {
					r, err := i()
					if err != nil {
						break
					}
					sortable.records = append(sortable.records, r)
				}
				sort.Sort(sortable)
				rs = RecordsToStream(sortable.records)
			}
			return rs()
		}
	}
}

func SortBy(keys []string) Filter {
	return Sort(func(r1, r2 Record) bool {
		for _, k := range keys {
			v1 := GetInt(r1, k)
			v2 := GetInt(r2, k)
			if v1 == v2 {
				continue
			}
			return v1 < v2
		}
		return false
	})
}

func Sum(dst, src string) Filter {
	return func(i Stream) Stream {
		var s int64
		var rs Stream
		return func() (Record, error) {
			if rs == nil {
				for {
					r, err := i()
					if err != nil {
						break
					}
					s += GetInt(r, src)
				}
				rs = RecordsToStream([]Record{{dst: Int(s)}})
			}
			return rs()
		}
	}
}

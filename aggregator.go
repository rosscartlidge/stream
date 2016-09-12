package stream

import "math"

type aggregatorOps struct {
	field   string
	process func(Record)
	finish  func()
	result  func() Stream
}

type Aggregator func() aggregatorOps

func FilterToAggregator(field string, f Filter) Aggregator {
	return func() aggregatorOps {
		rch := make(chan Record)
		sch := make(chan Stream)
		os := f(ChanToStream(rch))

		go func() {
			var records []Record
			for {
				r, err := os()
				if err != nil {
					break
				}
				records = append(records, r)
			}
			sch <- RecordsToStream(records)
		}()

		return aggregatorOps{
			field,
			func(r Record) {
				rch <- r
			},
			func() { close(rch) },
			func() Stream {
				return <-sch
			},
		}
	}
}

func ASum(field, dst, src string) Aggregator {
	return func() aggregatorOps {
		var s int64
		return aggregatorOps{
			field,
			func(r Record) {
				s += GetInt(r, src)
			},
			func() {},
			func() Stream {
				return RecordsToStream([]Record{{dst: Int(s)}})
			},
		}
	}
}

func AMin(field, dst, src string) Aggregator {
	return func() aggregatorOps {
		m := math.MaxInt64
		return aggregatorOps{
			field,
			func(r Record) {
				if v := GetInt(r, src); v < m {
					m := v
				}
			},
			func() {},
			func() Stream {
				return RecordsToStream([]Record{{dst: Int(m)}})
			},
		}
	}
}

func AMax(field, dst, src string) Aggregator {
	return func() aggregatorOps {
		m := math.MinInt64
		return aggregatorOps{
			field,
			func(r Record) {
				if v := GetInt(r, src); v < m {
					m := v
				}
			},
			func() {},
			func() Stream {
				return RecordsToStream([]Record{{dst: Int(m)}})
			},
		}
	}
}

func ACount(field, dst string) Aggregator {
	return func() aggregatorOps {
		var s int64
		return aggregatorOps{
			field,
			func(r Record) {
				s++
			},
			func() {},
			func() Stream {
				return RecordsToStream([]Record{{dst: Int(s)}})
			},
		}
	}
}

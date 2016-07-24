package stream

import (
	"strings"
)


func GroupBy(keys []string, as ...Aggregator) Filter {
	return func(i Stream) Stream {
		type group struct {
			record Record
			ops    []aggregatorOps
		}
		groups := make(map[string]group)
		var och chan Record
		return func() (Record, error) {
			if och == nil {
				for {
					r, err := i()
					if err != nil {
						break
					}
					var svs []string
					for _, f := range keys {
						svs = append(svs, GetString(r, f))
					}
					k := strings.Join(svs, ",")

					g, ok := groups[k]
					if !ok {
						record := make(Record)
						for _, f := range keys {
							record[f] = r[f]
						}
						var ops []aggregatorOps
						for _, a := range as {
							ops = append(ops, a())
						}
						g = group{
							record: record,
							ops:    ops,
						}
						groups[k] = g
					}
					for _, op := range g.ops {
						op.process(r)
					}
				}
				for _, g := range groups {
					for _, op := range g.ops {
						op.finish()
					}
				}
				och = make(chan Record, 1000)
				go func() {
					for _, g := range groups {
						for _, op := range g.ops {
							g.record[op.field] = op.result()
						}
						och <- g.record
					}
					close(och)
				}()
			}
			r, ok := <-och
			if !ok {
				return nil, EOS
			}
			return r, nil
		}
	}

}

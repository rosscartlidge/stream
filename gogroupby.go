package stream

import (
	"context"
	"strings"
)

func GofilterGroupBy(keys []string, as ...Aggregator) Gofilter {
	return func(ctx context.Context, out, in chan Record) {
		type group struct {
			record Record
			ops    []aggregatorOps
		}
		groups := make(map[string]group)
		for r := range in {
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
		for _, g := range groups {
			for _, op := range g.ops {
				g.record[op.field] = op.result()
			}
			out <- g.record
		}
	}
}

package main

import (
	"flag"
	"fmt"
	"os"

	stream "github.com/rosscartlidge/stream"
)

var nRecords = flag.Int("n", 10, "Number of records to generate")
var useGofilter = flag.Bool("g", false, "Use Gofilter")

func main() {
	flag.Parse()
	rs := stream.Enumerate(0, *nRecords, func(i int) []stream.Record {
		return []stream.Record{{"x": stream.Int(i), "y": stream.Int(i + 1)}}
	})
	if *useGofilter {
		gf := func(och, ich chan stream.Record) {
			for r := range ich {
				r["x"] = stream.Int(stream.GetInt(r, "x") + stream.GetInt(r, "y"))
				och <- r
			}
		}
		gf = stream.GofilterPipe(gf, gf)
		gf = stream.GofilterPipe(gf, gf)
		gf = stream.GofilterPipe(gf, gf)
		ich := make(chan stream.Record, 1000)
		go func() {
			for i := 0; i < *nRecords; i++ {
				ich <- stream.Record{"x": stream.Int(i), "y": stream.Int(i + 1)}
			}
			close(ich)
		}()
		och := make(chan stream.Record, 1000)
		go func() {
			gf(och, ich)
			close(och)
		}()
		for r := range och {
			fmt.Println(r)
		}
		return
		// f := stream.GofilterToFilter(gf)
		// rs = f(rs)
	} else {
		f := stream.Update(
			func(r stream.Record) stream.Record {
				r["x"] = stream.Int(stream.GetInt(r, "x") + stream.GetInt(r, "y"))
				return r
			},
		)
		f = stream.Pipe(f, f)
		f = stream.Pipe(f, f)
		f = stream.Pipe(f, f)
		rs = f(rs)
	}

	for {
		r, err := rs()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			break
		}
		fmt.Println(r)
	}
}

package main

import (
	"flag"
	"fmt"
	"os"

	stream "github.com/rosscartlidge/stream"
)

var nRecords = flag.Int("n", 10, "Number of records to generate")

func main() {
	flag.Parse()
	rs := stream.Enumerate(0, *nRecords, func(i int) []stream.Record {
		return []stream.Record{{"x": stream.Int(i), "y": stream.Int(i + 1)}}
	})
	f := stream.Update(
		func(r stream.Record) stream.Record {
			r["sum"] = stream.Int(stream.GetInt(r, "x") + stream.GetInt(r, "y"))
			return r
		},
	)
	rs = f(rs)
	for {
		r, err := rs()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			break
		}
		fmt.Println(r)
	}
}

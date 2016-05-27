package main

import (
	"flag"
	"os"

	"github.com/user/stream"
)

var nRecords = flag.Int("n", 10, "Number of records to generate")
var expand = flag.Bool("expand", true, "Run Expand")

func main() {
	flag.Parse()
	rs := stream.Enumerate(0, *nRecords, func(i int) []stream.Record {
		return []stream.Record{
			stream.RecordMap{"i": stream.Int(i), "x": stream.Int(i + 2)},
			stream.RecordMap{"i": stream.Int(i), "x": stream.Int(i - 1)},
		}
	})
	gs := stream.Pipe(
		stream.GroupBy([]string{"i"}, stream.ASum("sumx", "", "x"), stream.ASum("sumi", "", "i")),
		stream.Expand(),
		stream.SortBy([]string{"i"}),
	)(rs)
	stream.StreamToJSON(os.Stdout, gs)
}

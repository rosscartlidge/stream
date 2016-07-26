package main

import (
	"flag"
	"os"

	stream "github.com/rosscartlidge/newstream"
)

var nRecords = flag.Int("n", 10, "Number of records to generate")
var expand = flag.Bool("expand", true, "Run Expand")
var format = flag.String("format", "csv", "Output format")

func main() {
	flag.Parse()
	rs := stream.Enumerate(0, *nRecords, func(i int) []stream.Record {
		return []stream.Record{
			{"i": stream.Int(i), "x": stream.Int(i + 2)},
			{"i": stream.Int(i), "x": stream.Int(i - 1)},
		}
	})
	gs := stream.Pipe(
		stream.GroupBy([]string{"i"}, stream.ASum("sumx", "", "x"), stream.ASum("sumi", "", "i")),
		// stream.Expand(),
		stream.SortBy([]string{"i"}),
	)(rs)
	switch *format {
	case "csv":
		stream.StreamToCSV(os.Stdout, gs)
	case "json":
		stream.StreamToJSON(os.Stdout, gs)
	case "gob":
		stream.StreamToGob(os.Stdout, gs)
	}
}

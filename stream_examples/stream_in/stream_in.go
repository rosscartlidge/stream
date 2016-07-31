package main

import (
	"flag"
	"os"

	stream "github.com/rosscartlidge/stream"
)

var nRecords = flag.Int("n", 10, "Number of records to generate")
var expand = flag.Bool("expand", true, "Run Expand")
var format = flag.String("format", "csv", "Output format")

func main() {
	flag.Parse()
	var rs stream.Stream
	switch *format {
	case "csv":
		rs = stream.CSVToStream(os.Stdin)
	case "json":
		rs = stream.JSONToStream(os.Stdin)
	case "gob":
		rs = stream.GobToStream(os.Stdin)
	}
	gs := stream.Pipe(
		stream.Expand(),
		stream.GroupBy(nil, stream.ASum("sum_sumi", "", "sumi"), stream.ASum("sum_sumx", "", "sumx")), stream.Expand(),
	)(rs)
	stream.StreamToCSV(os.Stdout, gs)
}

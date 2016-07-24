package main

import (
	"flag"
	"os"

	stream "github.com/rosscartlidge/newstream"
)

var nRecords = flag.Int("n", 10, "Number of records to generate")
var expand = flag.Bool("expand", true, "Run Expand")
var csv = flag.Bool("csv", false, "Output CSV")

func main() {
	flag.Parse()
	var rs stream.Stream
	if *csv {
		rs = stream.CSVToStream(os.Stdin)
	} else {
		rs = stream.JSONToStream(os.Stdin)
	}
	gs := stream.Pipe(
		stream.GroupBy(nil, stream.ASum("sum_sumi", "", "sumi"), stream.ASum("sum_sumx", "", "sumx")), stream.Expand(),
	)(rs)
	stream.StreamToCSV(os.Stdout, gs)
}

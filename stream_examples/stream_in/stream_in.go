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
	rs := stream.JSONToStream(os.Stdin)
	gs := stream.Pipe(
		stream.GroupBy(nil, stream.ASum("sum_sumi", "", "sumi"), stream.ASum("sum_sumx", "", "sumx")), stream.Expand(),
	)(rs)
	stream.StreamToCSV(os.Stdout, gs)
}

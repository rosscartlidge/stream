package main

import (
	"flag"
	"os"

	stream "github.com/rosscartlidge/stream"
)

var input = flag.String("input", "json", "Input Format")

func main() {
	flag.Parse()
	var rs stream.Stream
	switch *input {
	case "csv":
		rs = stream.CSVToStream(os.Stdin)
	case "json":
		rs = stream.JSONToStream(os.Stdin)
	case "tsv":
		rs = stream.TSVToStream(os.Stdin, ",")
	}
	gs := stream.Pipe(
		stream.GroupBy([]string{"jupiter"}, stream.ACount("count", "")), stream.Expand(),
		stream.SortBy([]string{"count"}),
	)(rs)
	stream.StreamToCSV(os.Stdout, gs)
}

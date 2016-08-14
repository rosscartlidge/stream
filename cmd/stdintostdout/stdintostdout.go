package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/rosscartlidge/stream"
)

var ifmt = flag.String("ifmt", "json", "Input Format")
var ofmt = flag.String("ofmt", "json", "Output Format")

func main() {
	flag.Parse()
	var is stream.Stream
	switch *ifmt {
	case "csv":
		is = stream.CSVToStreamCh(os.Stdin)
	case "gob":
		is = stream.GobToStream(os.Stdin)
	case "json":
		is = stream.JSONToStream(os.Stdin)
	case "tsv":
		is = stream.TSVToStream(os.Stdin, "\t")
	default:
		fmt.Fprintln(os.Stderr, "Unknown format", *ifmt)
		os.Exit(2)
	}
	// is = stream.Go(stream.I)(is)
	switch *ofmt {
	case "csv":
		stream.StreamToCSV(os.Stdout, is)
	case "gob":
		stream.StreamToGob(os.Stdout, is)
	case "json":
		stream.StreamToJSON(os.Stdout, is)
	case "tsv":
		stream.StreamToTSV(os.Stdout, "\t", is)
	default:
		fmt.Fprintln(os.Stderr, "Unknown format", *ofmt)
		os.Exit(2)
	}
}

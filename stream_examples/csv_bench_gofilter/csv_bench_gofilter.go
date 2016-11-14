package main

import (
	"context"
	"flag"
	"os"

	stream "github.com/rosscartlidge/stream"
)

func main() {
	flag.Parse()
	gf := stream.GofilterPipe(
		stream.GofilterGroupBy([]string{"jupiter"}, stream.ACount("count", "")),
		stream.GofilterExpand(),
		stream.GofilterSortBy([]string{"count"}),
	)
	ctx := context.Background()
	in := make(chan stream.Record, 1000)
	go func() {
		stream.CSVToSource(ctx, in, os.Stdin)
		close(in)
	}()
	out := make(chan stream.Record, 1000)
	go func() {
		gf(ctx, out, in)
		close(out)
	}()
	stream.SinkToCSV(ctx, os.Stdout, out)
}

package main

import (
	"flag"
	"os"
	"sync"

	stream "github.com/rosscartlidge/newstream"
)

var nRecords = flag.Int("n", 10, "Number of records to generate")
var expand = flag.Bool("expand", true, "Run Expand")

func main() {
	flag.Parse()
	// stream.StreamToCSV(os.Stdout, Expand()(JsonToStream(os.Stdin)))
	rs := stream.Enumerate(0, *nRecords, func(i int) []stream.Record {
		return []stream.Record{
			{"i": stream.Int(i), "x": stream.Int(i + 2)},
			{"i": stream.Int(i), "x": stream.Int(i - 1)},
		}
	})
	rs1 := stream.Enumerate(*nRecords, *nRecords*2, func(i int) []stream.Record {
		return []stream.Record{
			{"i": stream.Int(i), "x": stream.Int(i + 2)},
			{"i": stream.Int(i), "x": stream.Int(i - 1)},
		}
	})
	rs = stream.Zip("", rs, rs1)
	// rs = stream.JsonToStream(os.Stdin)
	// tfile, _ := os.Create("/tmp/xxx")
	tch := make(chan stream.Record)
	gs := stream.Pipe(
		stream.Expand(),
		stream.GroupBy([]string{"i"}, stream.ASum("sumx", "", "x"), stream.ASum("sumi", "", "i")),
		func() stream.Filter {
			if *expand {
				return stream.Expand()
			} else {
				return stream.I
			}
		}(),
		stream.Tee(tch),
		stream.SortBy([]string{"i"}),
		stream.GroupBy(nil, stream.ASum("sum_sumi", "", "sumi"), stream.ASum("sum_sumx", "", "sumx")), stream.Expand(),
	)(rs)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream.StreamToCSV(os.Stdout, stream.ChanToStream(tch))
		/*
		for r := range tch {
			fmt.Println("XXX", r)
		}
		*/
	}()
	/*
	for {
		r, ok := gs()
		if !ok {
			break
		}
		fmt.Println(r)
	}
	*/
	stream.StreamToCSV(os.Stdout, gs)
	wg.Wait()
	// StreamToJSON(os.Stdout, gs)

}

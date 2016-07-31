package stream

import (
	"bufio"
	"io"
	"sort"
	"strings"
)

func TSVToStream(i io.Reader, sep string) Stream {
	ib := bufio.NewReader(i)
	var header []string
	return func() (Record, error) {
		if header == nil {
			l, err := ib.ReadString('\n')
			if err != nil {
				return nil, EOS
			}
			header = strings.Split(l[:len(l)-1], sep)
		}
		l, err := ib.ReadString('\n')
		if err != nil {
			return nil, EOS
		}
		data := strings.Split(l[:len(l)-1], sep)
		r := make(Record, len(header))
		for i, f := range header {
			r[f] = String(data[i])
		}
		return r, nil
	}
}

func StreamToTSV(o io.Writer, sep string, i Stream) {
	var header []string
	ob := bufio.NewWriter(o)
	for {
		r, err := i()
		if err != nil {
			break
		}
		if header == nil {
			for f := range r {
				header = append(header, f)
			}
			sort.Sort(sort.StringSlice(header))
			ob.Write([]byte(strings.Join(header, sep) + "\n"))
		}
		var l []string
		for _, f := range header {
			l = append(l, GetString(r, f))
		}
		ob.Write([]byte(strings.Join(l, sep) + "\n"))
	}
	ob.Flush()
}

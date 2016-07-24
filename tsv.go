package stream

import (
	"bufio"
	"io"
	"strings"
)
func TSVToStream(i io.Reader, sep string) Stream {
	r := bufio.NewReader(i)
	var header []string
	return func() (Record, error) {
		if header == nil {
			l, err := r.ReadString('\n')
			if err != nil {
				return nil, EOS
			}
			header = strings.Split(l[:len(l)-1], sep)
		}
		l, err := r.ReadString('\n')
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

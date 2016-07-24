package stream

import (
	"sync"
)

func Chain(fs ...Stream) Stream {
	return func() (Record, error) {
		for {
			if len(fs) == 0 {
				return nil, EOS
			}
			if r, err := fs[0](); err == nil {
				return r, nil
			}
			fs = fs[1:]
		}
	}
}

func Zip(field string, fs ...Stream) Stream {
	return func() (Record, error) {
		var row []Record
		for _, f := range fs {
			r, err := f()
			if err != nil {
				return nil, EOS
			}
			row = append(row, r)
		}
		return Record{field: RecordsToStream(row)}, nil
	}
}

func Merge(fs ...Stream) Stream {
	mch := make(chan Record, 1000)
	wg := sync.WaitGroup{}
	for _, f := range fs {
		wg.Add(1)
		go func(f Stream) {
			defer wg.Done()
			for {
				r, err := f()
				if err != nil {
					break
				}
				mch <- r
			}
		}(f)
	}
	go func() {
		wg.Wait()
		close(mch)
	}()
	return func() (Record, error) {
		r, ok := <-mch
		if !ok {
			return nil, EOS
		}
		return r, nil
	}
}

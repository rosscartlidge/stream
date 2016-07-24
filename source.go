package stream

func Enumerate(min, max int, f func(int) []Record) Stream {
	i := min
	var rs Stream
	return func() (Record, error) {
		for i < max {
			if rs == nil {
				rs = RecordsToStream(f(i))
			}
			if r, err := rs(); err == nil {
				return r, nil
			}
			rs = nil
			i++
		}
		return nil, EOS
	}
}

func RecordsToStream(records []Record) Stream {
	return func() (Record, error) {
		if len(records) == 0 {
			return nil, EOS
		}
		var r Record
		r, records = records[0], records[1:]
		return r, nil
	}
}

func ChanToStream(ch chan Record) Stream {
	return func() (Record, error) {
		if r, ok := <-ch; !ok {
			return nil, EOS
		} else {
			return r, nil
		}
	}
}

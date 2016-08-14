package stream

import "errors"

type Value interface {
	Int() (int64, error)
	Float() (float64, error)
	String() (string, error)
	Bool() (bool, error)
	Stream() (Stream, error)
}

type Record map[string]Value

type Stream func() (Record, error)

type Filter func(Stream) Stream

type Gofilter func(out, in chan Record)

func Update(f func(Record) Record) Filter {
	return func(i Stream) Stream {
		return func() (Record, error) {
			for {
				r, err := i()
				if err != nil {
					return nil, EOS
				}
				if nr := f(r); nr != nil {
					return nr, nil
				} else {
					continue
				}
			}
		}
	}
}

func Where(w func(Record) bool) Filter {
	return Update(func(r Record) Record {
		if !w(r) {
			return nil
		}
		return r
	})
}

func Pipe(fs ...Filter) Filter {
	switch l := len(fs); l {
	case 0:
		return I
	case 1:
		return fs[0]
	default:
		return func(s Stream) Stream {
			return fs[l-1](Pipe(fs[:l-1]...)(s))
		}
	}
}

func Tee(tch chan Record) Filter {
	return func(i Stream) Stream {
		return func() (Record, error) {
			r, err := i()
			if err != nil {
				close(tch)
				return nil, EOS
			}
			tch <- r
			return r, nil
		}
	}
}

var (
	EOS = errors.New("EOS")
	I   = Update(func(r Record) Record {
		return r
	})
)

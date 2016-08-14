package stream

import (
	"fmt"
	"os"
)

func Go(f Filter) Filter {
	return func(s Stream) Stream {
		ch := make(chan Record, 1000)
		s = f(s)
		go func() {
			for {
				if r, err := s(); err != nil {
					close(ch)
					break
				} else {
					ch <- r
				}
			}
		}()
		return ChanToStream(ch)
	}
}

func GoPipe(fs ...Filter) Filter {
	switch l := len(fs); l {
	case 0:
		return I
	case 1:
		return Go(fs[0])
	default:
		return func(s Stream) Stream {
			return Go(fs[l-1])(GoPipe(fs[:l-1]...)(s))
		}
	}
}

func GofilterToFilter(g Gofilter) Filter {
	return func(i Stream) Stream {
		fmt.Fprintln(os.Stderr, "make o,ich")
		och := make(chan Record, 1000)
		ich := make(chan Record, 1000)
		go func() {
			for {
				r, err := i()
				if err != nil {
					break
				}
				ich <- r
			}
			close(ich)
		}()
		go func() {
			g(och, ich)
			close(och)
		}()
		return func() (Record, error) {
			if r, ok := <-och; ok {
				return r, nil
			}
			return nil, EOS
		}
	}
}

func GofilterPipe(gfs ...Gofilter) Gofilter {
	switch l := len(gfs); l {
	case 0:
		return func(och, ich chan Record) {
			for r := range ich {
				och <- r
			}
		}
	case 1:
		return gfs[0]
	default:
		return func(och, ich chan Record) {
			jch := make(chan Record, 1000)
			fmt.Fprintln(os.Stderr, "make jch")
			go func() {
				gfs[0](jch, ich)
				close(jch)
			}()
			GofilterPipe(gfs[1:]...)(och, jch)
		}
	}
}

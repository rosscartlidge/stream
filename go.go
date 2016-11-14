package stream

import "context"

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
			g(context.TODO(), och, ich)
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
		return func(ctx context.Context, och, ich chan Record) {
			for r := range ich {
				och <- r
			}
		}
	case 1:
		return gfs[0]
	default:
		return func(ctx context.Context, och, ich chan Record) {
			jch := make(chan Record, 1000)
			go func() {
				gfs[0](ctx, jch, ich)
				close(jch)
			}()
			GofilterPipe(gfs[1:]...)(ctx, och, jch)
		}
	}
}

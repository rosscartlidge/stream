package stream

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

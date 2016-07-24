package stream

type Empty struct{}

func (e Empty) Int() (int64, error) {
	var d int64
	return d, nil
}

func (e Empty) Float() (float64, error) {
	var d float64
	return d, nil
}

func (e Empty) Bool() (bool, error) {
	var d bool
	return d, nil
}

func (e Empty) String() (string, error) {
	var d string
	return d, nil
}

func (e Empty) Stream() (Stream, error) {
	n := func() (Record, error) {
		return nil, EOS
	}
	return Stream(n), nil
}

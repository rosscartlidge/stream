package stream

func StreamToRecords(s Stream) []Record {
	var rs []Record
	for {
		r, err := s()
		if err != nil {
			break
		}
		rs = append(rs, r)
	}
	return rs
}

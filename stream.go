package stream

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Record interface {
	Has(string) bool
	Get(string) Value
	Delete(string)
	Set(string, Value)
	Fields() []string
	Copy() Record
}

type Value interface {
	ToInt(bool) (int64, error)
	ToFloat(bool) (float64, error)
	ToString(bool) (string, error)
	ToBool(bool) (bool, error)
	ToStream(bool) (Stream, error)
}

type Stream func() (Record, bool)

type Filter func(Stream) Stream

type aggregatorOps struct {
	field   string
	process func(Record)
	finish  func()
	result  func() Stream
}

type Aggregator func() aggregatorOps

// RecordMap
type RecordMap map[string]Value

func (m RecordMap) Has(s string) bool {
	_, ok := m[s]
	return ok
}

func (m RecordMap) Get(s string) Value {
	v, ok := m[s]
	if !ok {
		return Empty{}
	}
	return v
}

func (m RecordMap) Fields() []string {
	var fs []string
	for f := range m {
		fs = append(fs, f)
	}
	return fs
}

func (m RecordMap) Set(s string, v Value) {
	m[s] = v
}

func (m RecordMap) Delete(s string) {
	delete(m, s)
}

func (m RecordMap) Copy() Record {
	c := make(RecordMap)
	for f, v := range m {
		c[f] = v
	}
	return c
}

// RecordStack
type RecordStack struct {
	base Record
	diff map[string]Value
}

func (m RecordStack) Has(s string) bool {
	_, diff := m.diff[s]
	base := m.base.Has(s)
	return diff || base
}

func (m RecordStack) Get(s string) Value {
	v, ok := m.diff[s]
	if !ok {
		return m.Get(s)
	}
	return v
}

func (m RecordStack) Fields() []string {
	all := make(map[string]bool)
	for _, f := range m.base.Fields() {
		all[f] = true
	}
	for f := range m.diff {
		all[f] = true
	}
	var fs []string
	for f := range all {
		fs = append(fs, f)
	}
	return fs
}

func (m RecordStack) Set(s string, v Value) {
	m.diff[s] = v
}

func (m RecordStack) Delete(s string) {
	delete(m.diff, s)
}

func (m RecordStack) Copy() Record {
	diff := make(map[string]Value)
	for f, v := range m.diff {
		diff[f] = v
	}
	c := RecordStack{m.base, diff}
	return c
}

//

// Int
type Int int64

func (i Int) ToInt(convert bool) (int64, error) {
	return int64(i), nil
}

func (i Int) ToFloat(convert bool) (float64, error) {
	var d float64
	if convert {
		return float64(i), nil
	}
	return d, fmt.Errorf("Value is Int not Float")
}

func (i Int) ToString(convert bool) (string, error) {
	var d string
	if convert {
		return strconv.FormatInt(int64(i), 10), nil
	}
	return d, fmt.Errorf("Value is Int not String")
}

func (i Int) ToBool(convert bool) (bool, error) {
	var d bool
	if convert {
		return i != 0, nil
	}
	return d, fmt.Errorf("Value is Int not Bool")
}

func (i Int) ToStream(convert bool) (Stream, error) {
	if convert {
		m := RecordMap(map[string]Value{"Int": i})
		returned := false
		n := func() (Record, bool) {
			if returned {
				return nil, false
			}
			returned = true
			return m, true
		}
		return Stream(n), nil
	}
	d := func() (Record, bool) {
		return nil, false
	}
	return Stream(d), fmt.Errorf("Value is Int not Stream")
}

// Float
type Float float64

func (f Float) ToInt(convert bool) (int64, error) {
	var d int64
	if convert {
		return int64(f), nil
	}
	return d, fmt.Errorf("Value is Float not Int")
}

func (f Float) ToFloat(convert bool) (float64, error) {
	return float64(f), nil
}

func (f Float) ToString(convert bool) (string, error) {
	var d string
	if convert {
		return strconv.FormatFloat(float64(f), 'g', -1, 64), nil
	}
	return d, fmt.Errorf("Value is Float not String")
}

func (f Float) ToBool(convert bool) (bool, error) {
	var d bool
	if convert {
		return f != 0.0, nil
	}
	return d, fmt.Errorf("Value is Float not Bool")
}

func (f Float) ToStream(convert bool) (Stream, error) {
	if convert {
		m := RecordMap(map[string]Value{"Float": f})
		returned := false
		n := func() (Record, bool) {
			if returned {
				return nil, false
			}
			returned = true
			return m, true
		}
		return Stream(n), nil
	}
	d := func() (Record, bool) {
		return nil, false
	}
	return Stream(d), fmt.Errorf("Value is Float not Stream")

}

// String
type String string

func (s String) ToInt(convert bool) (int64, error) {
	var d int64
	if convert {
		i, err := strconv.ParseInt(string(s), 10, 64)
		return i, err
	}
	return d, fmt.Errorf("Value is String not Int")
}

func (s String) ToFloat(convert bool) (float64, error) {
	var d float64
	if convert {
		f, err := strconv.ParseFloat(string(s), 64)
		return f, err
	}
	return d, fmt.Errorf("Value is String not Float")
}

func (s String) ToString(convert bool) (string, error) {
	return string(s), nil
}

func (s String) ToBool(convert bool) (bool, error) {
	var d bool
	if convert {
		return s != "", nil
	}
	return d, fmt.Errorf("Value is String not Bool")
}

func (s String) ToStream(convert bool) (Stream, error) {
	if convert {
		m := RecordMap(map[string]Value{"String": s})
		returned := false
		n := func() (Record, bool) {
			if returned {
				return nil, false
			}
			returned = true
			return m, true
		}
		return Stream(n), nil
	}
	d := func() (Record, bool) {
		return nil, false
	}
	return Stream(d), fmt.Errorf("Value is String not Stream")
}

// Bool
type Bool bool

func (b Bool) ToInt(convert bool) (int64, error) {
	var d int64
	if convert {
		if b {
			return 1, nil
		} else {
			return 0, nil
		}
	}
	return d, fmt.Errorf("Value is Bool not Int")
}

func (b Bool) ToFloat(convert bool) (float64, error) {
	var d float64
	if convert {
		if b {
			return 1.0, nil
		} else {
			return 0.0, nil
		}
	}
	return d, fmt.Errorf("Value is Bool not Float")
}

func (b Bool) ToString(convert bool) (string, error) {
	var d string
	if convert {
		return strconv.FormatBool(bool(b)), nil
	}
	return d, fmt.Errorf("Value is Bool not String")
}

func (b Bool) ToBool(convert bool) (bool, error) {
	return bool(b), nil
}

func (b Bool) ToStream(convert bool) (Stream, error) {
	if convert {
		m := RecordMap(map[string]Value{"Bool": b})
		returned := false
		n := func() (Record, bool) {
			if returned {
				return nil, false
			}
			returned = true
			return m, true
		}
		return Stream(n), nil
	}
	d := func() (Record, bool) {
		return nil, false
	}
	return Stream(d), fmt.Errorf("Value is Bool not Stream")
}

//
// Stream
func (s Stream) ToInt(convert bool) (int64, error) {
	var d int64
	return d, fmt.Errorf("Value is Stream not Int")
}

func (s Stream) ToFloat(convert bool) (float64, error) {
	var d float64
	return d, fmt.Errorf("Value is Stream not Float")
}

func (s Stream) ToBool(convert bool) (bool, error) {
	var d bool
	return d, fmt.Errorf("Value is Stream not Bool")
}

func (s Stream) ToString(convert bool) (string, error) {
	var d string
	return d, fmt.Errorf("Value is Stream not String")
}

func (s Stream) ToStream(convert bool) (Stream, error) {
	return s, nil
}

func (s Stream) MarshalJSON() ([]byte, error) {
	return json.Marshal(StreamToRecords(s))
}

// Empty
type Empty struct{}

func (e Empty) ToInt(convert bool) (int64, error) {
	var d int64
	if convert {
		return d, nil
	}
	return d, fmt.Errorf("Value is Empty not Int")
}

func (e Empty) ToFloat(convert bool) (float64, error) {
	var d float64
	if convert {
		return d, nil
	}
	return d, fmt.Errorf("Value is Empty not Float")
}

func (e Empty) ToBool(convert bool) (bool, error) {
	var d bool
	if convert {
		return d, nil
	}
	return d, fmt.Errorf("Value is Empty not Bool")
}

func (e Empty) ToString(convert bool) (string, error) {
	var d string
	if convert {
		return d, nil
	}
	return d, fmt.Errorf("Value is Empty not String")
}

func (e Empty) ToStream(convert bool) (Stream, error) {
	n := func() (Record, bool) {
		return nil, false
	}
	if convert {
		return Stream(n), nil
	}
	return Stream(n), fmt.Errorf("Value is Empty not Stream")
}

func RecordsToStream(records []Record) Stream {
	return func() (Record, bool) {
		if len(records) == 0 {
			return nil, false
		}
		var r Record
		r, records = records[0], records[1:]
		return r, true
	}
}

func StreamToRecords(s Stream) []Record {
	var rs []Record
	for {
		r, ok := s()
		if !ok {
			break
		}
		rs = append(rs, r)
	}
	return rs
}

func Enumerate(min, max int, f func(int) []Record) Stream {
	i := min
	var rs Stream
	return func() (Record, bool) {
		for i < max {
			if rs == nil {
				rs = RecordsToStream(f(i))
			}
			if r, ok := rs(); ok {
				return r, true
			}
			rs = nil
			i++
		}
		return nil, false
	}
}

func Update(f func(Record) Record) Filter {
	return func(i Stream) Stream {
		return func() (Record, bool) {
			for {
				r, ok := i()
				if !ok {
					return nil, false
				}
				if nr := f(r); nr != nil {
					return nr, true
				} else {
					continue
				}
			}
		}
	}
}

var I = Update(func(r Record) Record {
	return r
})

type RecordSort struct {
	records []Record
	less    func(r1, r2 Record) bool
}

func (rs RecordSort) Len() int {
	return len(rs.records)
}

func (rs RecordSort) Less(i, j int) bool {
	return rs.less(rs.records[i], rs.records[j])
}

func (rs RecordSort) Swap(i, j int) {
	rs.records[i], rs.records[j] = rs.records[j], rs.records[i]
}

func Sort(less func(r1, r2 Record) bool) Filter {
	return func(i Stream) Stream {
		sortable := RecordSort{less: less}
		var rs Stream
		return func() (Record, bool) {
			if rs == nil {
				for {
					r, ok := i()
					if !ok {
						break
					}
					sortable.records = append(sortable.records, r)
				}
				sort.Sort(sortable)
				rs = RecordsToStream(sortable.records)
			}
			return rs()
		}
	}
}

func SortBy(keys []string) Filter {
	return Sort(func(r1, r2 Record) bool {
		for _, k := range keys {
			v1, _ := r1.Get(k).ToInt(true)
			v2, _ := r2.Get(k).ToInt(true)
			if v1 == v2 {
				continue
			}
			return v1 < v2
		}
		return false
	})
}

func FilterToAggregator(field string, f Filter) Aggregator {
	return func() aggregatorOps {
		rch := make(chan Record)
		sch := make(chan Stream)
		os := f(ChanToStream(rch))

		go func() {
			var records []Record
			for {
				r, ok := os()
				if !ok {
					break
				}
				records = append(records, r)
			}
			sch <- RecordsToStream(records)
		}()

		return aggregatorOps{
			field,
			func(r Record) {
				rch <- r
			},
			func() { close(rch) },
			func() Stream {
				return <-sch
			},
		}
	}
}

func ASum(field, dst, src string) Aggregator {
	return func() aggregatorOps {
		var s int64
		return aggregatorOps{
			field,
			func(r Record) {
				v, _ := r.Get(src).ToInt(true)
				s += v
			},
			func() {},
			func() Stream {
				return RecordsToStream([]Record{RecordMap{dst: Int(s)}})
			},
		}
	}
}

func Sum(dst, src string) Filter {
	return func(i Stream) Stream {
		var s int64
		var rs Stream
		return func() (Record, bool) {
			if rs == nil {
				for {
					r, ok := i()
					if !ok {
						break
					}
					v, _ := r.Get(src).ToInt(true)
					s += v
				}
				rs = RecordsToStream([]Record{RecordMap{dst: Int(s)}})
			}
			return rs()
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

func Go(f Filter) Filter {
	return func(s Stream) Stream {
		ch := make(chan Record)
		s = f(s)
		go func() {
			for {
				if r, ok := s(); !ok {
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

func GoPipe(fs ...Filter) Filter {
	switch l := len(fs); l {
	case 0:
		return Update(func(r Record) Record {
			return r
		})
	case 1:
		return Go(fs[0])
	default:
		return func(s Stream) Stream {
			return Go(fs[l-1])(GoPipe(fs[:l-1]...)(s))
		}
	}
}

func GetString(r Record, f string) string {
	v, _ := r.Get(f).ToString(true)
	return v
}

func ChanToStream(ch chan Record) Stream {
	return func() (Record, bool) {
		if r, ok := <-ch; !ok {
			return nil, false
		} else {
			return r, true
		}
	}
}

func GroupBy(keys []string, as ...Aggregator) Filter {
	return func(i Stream) Stream {
		type group struct {
			record Record
			ops    []aggregatorOps
		}
		groups := make(map[string]group)
		var och chan Record
		return func() (Record, bool) {
			if och == nil {
				for {
					r, ok := i()
					if !ok {
						break
					}
					var svs []string
					for _, f := range keys {
						svs = append(svs, GetString(r, f))
					}
					k := strings.Join(svs, ",")

					g, ok := groups[k]
					if !ok {
						record := make(RecordMap)
						for _, f := range keys {
							record.Set(f, r.Get(f))
						}
						var ops []aggregatorOps
						for _, a := range as {
							ops = append(ops, a())
						}
						g = group{
							record: record,
							ops:    ops,
						}
						groups[k] = g
					}
					for _, op := range g.ops {
						op.process(r)
					}
				}
				for _, g := range groups {
					for _, op := range g.ops {
						op.finish()
					}
				}
				och = make(chan Record, 1000)
				go func() {
					for _, g := range groups {
						for _, op := range g.ops {
							g.record.Set(op.field, op.result())
						}
						och <- g.record
					}
					close(och)
				}()
			}
			r, ok := <-och
			return r, ok
		}
	}

}

func Expand() Filter {
	return func(i Stream) Stream {
		var expanded Stream
		return func() (Record, bool) {
			for {
				if expanded == nil {
					if r, ok := i(); !ok {
						return nil, false
					} else {
						expanded = RecordsToStream(Flatten(r, "."))
					}
				}
				if r, ok := expanded(); ok {
					return r, true
				}
				expanded = nil
			}
		}
	}
}

func ToCSV(o io.Writer) Filter {
	return func(i Stream) Stream {
		return func() (Record, bool) {
			var records []Record
			fm := make(map[string]bool)
			for {
				r, ok := i()
				if !ok {
					break
				}
				for _, f := range r.Fields() {
					fm[f] = true
				}
				records = append(records, r)
			}
			var fs []string
			for f := range fm {
				fs = append(fs, f)
			}
			sort.Sort(sort.StringSlice(fs))
			w := csv.NewWriter(o)
			w.Write(fs)
			for _, r := range records {
				var l []string
				for _, f := range fs {
					v, _ := r.Get(f).ToString(true)
					l = append(l, v)
				}
				w.Write(l)
			}
			w.Flush()
			return nil, false
		}
	}
}

func Tee(tch chan Record) Filter {
	return func(i Stream) Stream {
		return func() (Record, bool) {
			r, ok := i()
			if !ok {
				close(tch)
				return nil, false
			}
			tch <- r
			return r, true
		}
	}
}

func Chain(fs ...Stream) Stream {
	return func() (Record, bool) {
		for {
			if len(fs) == 0 {
				return nil, false
			}
			if r, ok := fs[0](); ok {
				return r, true
			}
			fs = fs[1:]
		}
	}
}

func Zip(field string, fs ...Stream) Stream {
	return func() (Record, bool) {
		var row []Record
		for _, f := range fs {
			r, ok := f()
			if !ok {
				return nil, false
			}
			row = append(row, r)
		}
		return RecordMap{field: RecordsToStream(row)}, true
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
				r, ok := f()
				if !ok {
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
	return func() (Record, bool) {
		r, ok := <-mch
		return r, ok
	}
}

func StreamToCSV(o io.Writer, i Stream) {
	var records []Record
	fm := make(map[string]bool)
	for {
		r, ok := i()
		if !ok {
			break
		}
		for _, f := range r.Fields() {
			fm[f] = true
		}
		records = append(records, r)
	}
	var fs []string
	for f := range fm {
		fs = append(fs, f)
	}
	sort.Sort(sort.StringSlice(fs))
	w := csv.NewWriter(o)
	w.Write(fs)
	for _, r := range records {
		var l []string
		for _, f := range fs {
			v, _ := r.Get(f).ToString(true)
			l = append(l, v)
		}
		w.Write(l)
	}
	w.Flush()
}

func StreamToJSON(o io.Writer, i Stream) {
	for {
		r, ok := i()
		if !ok {
			break
		}
		b, err := json.Marshal(r)
		if err != nil {
			fmt.Println("error:", err)
		}
		os.Stdout.Write(b)
	}
}

func NewRecordMap(m map[string]interface{}) RecordMap {
	r := RecordMap{}
	for f, x := range m {
		switch v := x.(type) {
		case int64:
			r.Set(f, Int(v))
		case float64:
			r.Set(f, Float(v))
		case string:
			r.Set(f, String(v))
		case bool:
			r.Set(f, Bool(v))
		case []interface{}:
			var rs []Record
			for _, x := range v {
				m := x.(map[string]interface{})
				rs = append(rs, NewRecordMap(m))
			}
			r.Set(f, RecordsToStream(rs))
		}
	}
	return r
}

func JSONToStream(i io.Reader) Stream {
	d := json.NewDecoder(i)
	return func() (Record, bool) {
		var m map[string]interface{}
		if err := d.Decode(&m); err != nil {
			return nil, false
		}
		r := NewRecordMap(m)
		return r, true
	}
}

func cross(columns [][]Record) []Record {
	if len(columns) == 0 {
		return nil
	}
	if len(columns) == 1 {
		return columns[0]
	}
	var rs []Record
	for _, lr := range cross(columns[1:]) {
		for _, rr := range columns[0] {
			r := rr.Copy()
			for _, f := range lr.Fields() {
				r.Set(f, lr.Get(f))
			}
			rs = append(rs, r)
		}
	}
	return rs
}

func Flatten(r Record, sep string) []Record {
	var columns [][]Record
	var fs []string
	for _, f := range r.Fields() {
		if s, ok := r.Get(f).(Stream); ok {
			var rs []Record
			for {
				if r, ok := s(); ok {
					if f != "" {
						for _, fi := range r.Fields() {
							ef := []string{f}
							if fi != "" {
								ef = append(ef, fi)
							}
							r.Set(strings.Join(ef, sep), r.Get(fi))
							r.Delete(fi)
						}
					}
					rs = append(rs, Flatten(r, sep)...)
				} else {
					break
				}
			}
			columns = append(columns, rs)
		} else {
			fs = append(fs, f)
		}
	}
	if len(columns) == 0 {
		return []Record{r}
	}
	crs := cross(columns)
	if len(fs) == 0 {
		return crs
	}
	for _, cr := range crs {
		for _, f := range fs {
			cr.Set(f, r.Get(f))
		}
	}
	return crs
}

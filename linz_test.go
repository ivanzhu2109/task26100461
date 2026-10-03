package waitlist

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

// ---- sequential reference model ----

type mreq struct {
	ticket   int64
	student  string
	section  string
	enrolled bool
}

type model struct {
	specs    map[string]SectionSpec
	enrolled map[string]int // section -> enrolled count
	stuCred  map[string]int // student -> credits
	stuEnr   map[string]map[string]bool
	stuWait  map[string]map[string]int64 // student -> section -> ticket
	reqs     map[int64]*mreq
	secReqs  map[string]map[int64]*mreq // section -> live requests
}

func newModel(students []string, sections map[string]SectionSpec) *model {
	m := &model{
		specs:    sections,
		enrolled: map[string]int{},
		stuCred:  map[string]int{},
		stuEnr:   map[string]map[string]bool{},
		stuWait:  map[string]map[string]int64{},
		reqs:     map[int64]*mreq{},
		secReqs:  map[string]map[int64]*mreq{},
	}
	for _, st := range students {
		m.stuEnr[st] = map[string]bool{}
		m.stuWait[st] = map[string]int64{}
	}
	for sec := range sections {
		m.secReqs[sec] = map[int64]*mreq{}
	}
	return m
}

func (m *model) promoteClosure(secs map[string]bool) {
	for {
		var best *mreq
		bestSec := ""
		for sec := range secs {
			spec := m.specs[sec]
			if m.enrolled[sec] >= spec.Capacity {
				continue
			}
			for _, r := range m.secReqs[sec] {
				if r.enrolled {
					continue
				}
				if m.stuCred[r.student]+spec.Credits > MaxCredits {
					continue
				}
				if best == nil || r.ticket < best.ticket {
					best, bestSec = r, sec
				}
			}
		}
		if best == nil {
			return
		}
		best.enrolled = true
		m.enrolled[bestSec]++
		m.stuCred[best.student] += m.specs[bestSec].Credits
		m.stuEnr[best.student][bestSec] = true
		delete(m.stuWait[best.student], bestSec)
	}
}

func (m *model) enqueue(ticket int64, student, section string) {
	r := &mreq{ticket: ticket, student: student, section: section}
	m.reqs[ticket] = r
	m.secReqs[section][ticket] = r
	m.stuWait[student][section] = ticket
	m.promoteClosure(map[string]bool{section: true})
}

func (m *model) cancel(ticket int64) {
	r := m.reqs[ticket]
	if r == nil {
		return
	}
	delete(m.reqs, ticket)
	delete(m.secReqs[r.section], ticket)
	if r.enrolled {
		spec := m.specs[r.section]
		m.enrolled[r.section]--
		m.stuCred[r.student] -= spec.Credits
		delete(m.stuEnr[r.student], r.section)
	} else {
		delete(m.stuWait[r.student], r.section)
	}
	affected := map[string]bool{r.section: true}
	for sec := range m.stuWait[r.student] {
		affected[sec] = true
	}
	m.promoteClosure(affected)
}

func (m *model) inspect(students, sections []string) Snapshot {
	snap := Snapshot{Students: map[string]StudentView{}, Sections: map[string]SectionView{}}
	for _, st := range students {
		enr := []string{}
		for sec := range m.stuEnr[st] {
			enr = append(enr, sec)
		}
		sort.Strings(enr)
		snap.Students[st] = StudentView{Credits: m.stuCred[st], Enrolled: enr}
	}
	for _, sec := range sections {
		wait := []int64{}
		for _, r := range m.secReqs[sec] {
			if !r.enrolled {
				wait = append(wait, r.ticket)
			}
		}
		sort.Slice(wait, func(i, j int) bool { return wait[i] < wait[j] })
		spec := m.specs[sec]
		snap.Sections[sec] = SectionView{
			Capacity: spec.Capacity, Credits: spec.Credits,
			Enrolled: m.enrolled[sec], Waitlist: wait,
		}
	}
	return snap
}

// canonical key for memoization
func (m *model) key() string {
	var b []byte
	tks := make([]int64, 0, len(m.reqs))
	for tk := range m.reqs {
		tks = append(tks, tk)
	}
	sort.Slice(tks, func(i, j int) bool { return tks[i] < tks[j] })
	for _, tk := range tks {
		r := m.reqs[tk]
		fmt.Fprintf((*byteBuf)(&b), "%d:%s:%s:%v;", tk, r.student, r.section, r.enrolled)
	}
	return string(b)
}

type byteBuf []byte

func (b *byteBuf) Write(p []byte) (int, error) { *b = append(*b, p...); return len(p), nil }

// modelState is a deep copy of the mutable model state, used by the
// checker to backtrack.
type modelState struct {
	enrolled map[string]int
	stuCred  map[string]int
	stuEnr   map[string]map[string]bool
	stuWait  map[string]map[string]int64
	reqs     map[int64]*mreq
	secReqs  map[string]map[int64]*mreq
}

func (m *model) save() modelState {
	st := modelState{
		enrolled: map[string]int{},
		stuCred:  map[string]int{},
		stuEnr:   map[string]map[string]bool{},
		stuWait:  map[string]map[string]int64{},
		reqs:     map[int64]*mreq{},
		secReqs:  map[string]map[int64]*mreq{},
	}
	for k, v := range m.enrolled {
		st.enrolled[k] = v
	}
	for k, v := range m.stuCred {
		st.stuCred[k] = v
	}
	for k, v := range m.stuEnr {
		cp := map[string]bool{}
		for kk, vv := range v {
			cp[kk] = vv
		}
		st.stuEnr[k] = cp
	}
	for k, v := range m.stuWait {
		cp := map[string]int64{}
		for kk, vv := range v {
			cp[kk] = vv
		}
		st.stuWait[k] = cp
	}
	for k, v := range m.reqs {
		cp := *v
		st.reqs[k] = &cp
	}
	for k, v := range m.secReqs {
		cp := map[int64]*mreq{}
		for kk := range v {
			cp[kk] = st.reqs[kk] // keep aliasing with st.reqs consistent
		}
		st.secReqs[k] = cp
	}
	return st
}

func (m *model) restore(st modelState) {
	m.enrolled = st.enrolled
	m.stuCred = st.stuCred
	m.stuEnr = st.stuEnr
	m.stuWait = st.stuWait
	m.reqs = st.reqs
	m.secReqs = st.secReqs
}

// ---- history recording ----

type opKind int

const (
	opEnqueue opKind = iota
	opCancel
	opInspect
)

type op struct {
	kind     opKind
	ticket   int64
	student  string
	section  string
	students []string
	sections []string
	result   Snapshot // for inspect
}

type entry struct {
	op           op
	invoke, resp int64
}

type recorder struct {
	seq     int64
	mu      sync.Mutex
	entries []entry
}

func (r *recorder) log(o op, invoke, resp int64) {
	r.mu.Lock()
	r.entries = append(r.entries, entry{op: o, invoke: invoke, resp: resp})
	r.mu.Unlock()
}

func (r *recorder) enqueue(s *System, ticket int64, student, section string) {
	inv := atomic.AddInt64(&r.seq, 1)
	s.Enqueue(ticket, student, section)
	resp := atomic.AddInt64(&r.seq, 1)
	r.log(op{kind: opEnqueue, ticket: ticket, student: student, section: section}, inv, resp)
}

func (r *recorder) cancel(s *System, ticket int64) {
	inv := atomic.AddInt64(&r.seq, 1)
	s.Cancel(ticket)
	resp := atomic.AddInt64(&r.seq, 1)
	r.log(op{kind: opCancel, ticket: ticket}, inv, resp)
}

func (r *recorder) inspect(s *System, students, sections []string) Snapshot {
	inv := atomic.AddInt64(&r.seq, 1)
	snap := s.Inspect(students, sections)
	resp := atomic.AddInt64(&r.seq, 1)
	r.log(op{kind: opInspect, students: students, sections: sections, result: snap}, inv, resp)
	return snap
}

// ---- snapshot equality (nil and empty slices are equivalent) ----

func normStrings(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

func normTickets(v []int64) []int64 {
	if v == nil {
		return []int64{}
	}
	return v
}

func equalSnapshot(a, b Snapshot, students, sections []string) bool {
	for _, st := range students {
		av, bv := a.Students[st], b.Students[st]
		if av.Credits != bv.Credits ||
			!reflect.DeepEqual(normStrings(av.Enrolled), normStrings(bv.Enrolled)) {
			return false
		}
	}
	for _, sec := range sections {
		av, bv := a.Sections[sec], b.Sections[sec]
		if av.Capacity != bv.Capacity || av.Credits != bv.Credits ||
			av.Enrolled != bv.Enrolled ||
			!reflect.DeepEqual(normTickets(av.Waitlist), normTickets(bv.Waitlist)) {
			return false
		}
	}
	return true
}

// ---- linearizability checker (Wing-Gong style backtracking) ----

func checkLinearizable(t *testing.T, students []string, sections map[string]SectionSpec, entries []entry) bool {
	t.Helper()
	sort.Slice(entries, func(i, j int) bool { return entries[i].invoke < entries[j].invoke })
	n := len(entries)
	// before[i][j]: i must be linearized before j
	before := make([][]bool, n)
	for i := range before {
		before[i] = make([]bool, n)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if entries[i].resp < entries[j].invoke {
				before[i][j] = true
			}
		}
	}
	m := newModel(students, sections)
	linearized := make([]bool, n)
	memo := map[string]bool{}
	var search func(done int) bool
	search = func(done int) bool {
		if done == n {
			return true
		}
		for i := 0; i < n; i++ {
			if linearized[i] {
				continue
			}
			ready := true
			for k := 0; k < n; k++ {
				if before[k][i] && !linearized[k] {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			saved := m.save()
			ok := m.apply(entries[i].op)
			if ok {
				linearized[i] = true
				key := memoKey(linearized, m)
				if !memo[key] {
					if search(done + 1) {
						return true
					}
					memo[key] = true
				}
				linearized[i] = false
			}
			m.restore(saved)
		}
		return false
	}
	return search(0)
}

func memoKey(linearized []bool, m *model) string {
	var b []byte
	for _, v := range linearized {
		if v {
			b = append(b, '1')
		} else {
			b = append(b, '0')
		}
	}
	return string(b) + "|" + m.key()
}

// apply runs the op against the model; for inspect it verifies the
// recorded result. Returns false if the op cannot be linearized here.
func (m *model) apply(o op) bool {
	switch o.kind {
	case opEnqueue:
		m.enqueue(o.ticket, o.student, o.section)
		return true
	case opCancel:
		m.cancel(o.ticket)
		return true
	case opInspect:
		got := m.inspect(o.students, o.sections)
		return equalSnapshot(got, o.result, o.students, o.sections)
	}
	return false
}

// ---- checker self-tests ----

func TestCheckerAcceptsSequentialHistory(t *testing.T) {
	students := []string{"a", "b"}
	sections := map[string]SectionSpec{"A": {Capacity: 1, Credits: 3}}
	m := newModel(students, sections)
	m.enqueue(1, "a", "A")
	m.enqueue(2, "b", "A")
	want := m.inspect(students, []string{"A"})
	entries := []entry{
		{op: op{kind: opEnqueue, ticket: 1, student: "a", section: "A"}, invoke: 1, resp: 2},
		{op: op{kind: opEnqueue, ticket: 2, student: "b", section: "A"}, invoke: 3, resp: 4},
		{op: op{kind: opInspect, students: students, sections: []string{"A"}, result: want}, invoke: 5, resp: 6},
	}
	if !checkLinearizable(t, students, sections, entries) {
		t.Fatal("valid history rejected")
	}
}

func TestCheckerRejectsImpossibleHistory(t *testing.T) {
	students := []string{"a", "b"}
	sections := map[string]SectionSpec{"A": {Capacity: 1, Credits: 3}}
	// Two enqueues into a 1-seat section, then an inspect claiming both
	// enrolled: impossible under any serial order.
	bogus := Snapshot{
		Students: map[string]StudentView{
			"a": {Credits: 3, Enrolled: []string{"A"}},
			"b": {Credits: 3, Enrolled: []string{"A"}},
		},
		Sections: map[string]SectionView{
			"A": {Capacity: 1, Credits: 3, Enrolled: 2, Waitlist: []int64{}},
		},
	}
	entries := []entry{
		{op: op{kind: opEnqueue, ticket: 1, student: "a", section: "A"}, invoke: 1, resp: 2},
		{op: op{kind: opEnqueue, ticket: 2, student: "b", section: "A"}, invoke: 3, resp: 4},
		{op: op{kind: opInspect, students: students, sections: []string{"A"}, result: bogus}, invoke: 5, resp: 6},
	}
	if checkLinearizable(t, students, sections, entries) {
		t.Fatal("impossible history accepted")
	}
}

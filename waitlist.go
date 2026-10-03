// Package waitlist implements an in-process course-enrollment waitlist
// component with fine-grained locking and linearizable operations.
package waitlist

import (
	"fmt"
	"sort"
	"sync"
)

// MaxCredits is the per-student credit cap.
const MaxCredits = 21

// SectionSpec describes a teaching section at construction time.
type SectionSpec struct {
	Capacity int // fixed number of seats
	Credits  int // integer course credits, 1..6
}

// StudentView is a consistent read view of one student.
type StudentView struct {
	Credits  int      // total credits of enrolled sections
	Enrolled []string // enrolled section names, sorted
}

// SectionView is a consistent read view of one section.
type SectionView struct {
	Capacity int
	Credits  int
	Enrolled int     // number of enrolled students
	Waitlist []int64 // waiting tickets, ascending priority order
}

// Snapshot is the result of Inspect.
type Snapshot struct {
	Students map[string]StudentView
	Sections map[string]SectionView
}

// System is the waitlist component. Safe for concurrent use.
type System struct {
	reg        sync.Mutex // guards parent, comp, tickets, nextCompID (short critical sections only)
	parent     map[entityID]entityID
	comp       map[entityID]*component
	tickets    map[int64]*request
	nextCompID int64

	students map[string]*studentState
	sections map[string]*sectionState
}

// testHook, when non-nil, is invoked inside the mutation critical
// section of Enqueue and Cancel (component lock held). Test-only.
var testHook func(op string, ticket int64)

// New builds a System. Initially nobody is enrolled anywhere.
func New(students []string, sections map[string]SectionSpec) *System {
	s := &System{
		parent:   map[entityID]entityID{},
		comp:     map[entityID]*component{},
		tickets:  map[int64]*request{},
		students: map[string]*studentState{},
		sections: map[string]*sectionState{},
	}
	addEnt := func(e entityID) {
		s.parent[e] = e
		s.comp[e] = &component{id: s.nextCompID}
		s.nextCompID++
	}
	for _, name := range students {
		if _, dup := s.students[name]; dup {
			panic("waitlist: duplicate student " + name)
		}
		s.students[name] = &studentState{
			name:     name,
			enrolled: map[string]int64{},
			waiting:  map[string]int64{},
		}
		addEnt(studentEnt(name))
	}
	for name, spec := range sections {
		if _, dup := s.sections[name]; dup {
			panic("waitlist: duplicate section " + name)
		}
		if spec.Credits < 1 || spec.Credits > 6 {
			panic(fmt.Sprintf("waitlist: section %s credits %d out of range 1..6", name, spec.Credits))
		}
		if spec.Capacity < 0 {
			panic(fmt.Sprintf("waitlist: section %s negative capacity", name))
		}
		s.sections[name] = &sectionState{
			name: name, capacity: spec.Capacity, credits: spec.Credits,
			requests: map[int64]*request{},
		}
		addEnt(sectionEnt(name))
	}
	return s
}

// Enqueue submits an enrollment request. ticket is a unique positive
// integer; smaller tickets have higher promotion priority.
func (s *System) Enqueue(ticket int64, student, section string) {
	if _, ok := s.students[student]; !ok {
		panic("waitlist: unknown student " + student)
	}
	sec, ok := s.sections[section]
	if !ok {
		panic("waitlist: unknown section " + section)
	}
	if ticket <= 0 {
		panic(fmt.Sprintf("waitlist: non-positive ticket %d", ticket))
	}
	c := s.lockTwo(studentEnt(student), sectionEnt(section))
	defer c.mu.Unlock()
	if testHook != nil {
		testHook("enqueue", ticket)
	}
	s.reg.Lock()
	_, dup := s.tickets[ticket]
	if !dup {
		r := &request{ticket: ticket, student: student, section: section}
		s.tickets[ticket] = r
		sec.requests[ticket] = r
		s.students[student].waiting[section] = ticket
	}
	s.reg.Unlock()
	if dup {
		panic(fmt.Sprintf("waitlist: ticket %d reused", ticket))
	}
	s.promoteClosure(map[string]bool{section: true})
}

// Cancel revokes a waiting or enrolled request, releasing seat and
// credits if enrolled. Repeated cancels are no-ops.
func (s *System) Cancel(ticket int64) {
	s.reg.Lock()
	r := s.tickets[ticket]
	s.reg.Unlock()
	if r == nil {
		return
	}
	c := s.lockOne(sectionEnt(r.section))
	defer c.mu.Unlock()
	if testHook != nil {
		testHook("cancel", ticket)
	}
	s.reg.Lock()
	cur := s.tickets[ticket]
	if cur != nil {
		delete(s.tickets, ticket)
	}
	s.reg.Unlock()
	if cur == nil {
		return // concurrently cancelled already
	}
	sec := s.sections[r.section]
	st := s.students[r.student]
	if r.enrolled {
		sec.enrolled--
		st.credits -= sec.credits
		delete(st.enrolled, sec.name)
	} else {
		delete(st.waiting, sec.name)
	}
	delete(sec.requests, ticket)
	affected := map[string]bool{sec.name: true}
	for name := range st.waiting {
		affected[name] = true
	}
	s.promoteClosure(affected)
}

// Inspect returns a consistent snapshot of the given students and sections.
func (s *System) Inspect(students, sections []string) Snapshot {
	entities := make([]entityID, 0, len(students)+len(sections))
	for _, name := range students {
		if _, ok := s.students[name]; !ok {
			panic("waitlist: unknown student " + name)
		}
		entities = append(entities, studentEnt(name))
	}
	for _, name := range sections {
		if _, ok := s.sections[name]; !ok {
			panic("waitlist: unknown section " + name)
		}
		entities = append(entities, sectionEnt(name))
	}
	comps := s.lockAll(entities)
	defer unlockAll(comps)
	snap := Snapshot{
		Students: map[string]StudentView{},
		Sections: map[string]SectionView{},
	}
	for _, name := range students {
		st := s.students[name]
		enr := make([]string, 0, len(st.enrolled))
		for sec := range st.enrolled {
			enr = append(enr, sec)
		}
		sort.Strings(enr)
		snap.Students[name] = StudentView{Credits: st.credits, Enrolled: enr}
	}
	for _, name := range sections {
		sec := s.sections[name]
		var wait []int64
		for _, r := range sec.requests {
			if !r.enrolled {
				wait = append(wait, r.ticket)
			}
		}
		sort.Slice(wait, func(i, j int) bool { return wait[i] < wait[j] })
		snap.Sections[name] = SectionView{
			Capacity: sec.capacity, Credits: sec.credits,
			Enrolled: sec.enrolled, Waitlist: wait,
		}
	}
	return snap
}

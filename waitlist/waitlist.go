// Package waitlist implements an in-process course-enrollment waitlist
// component with fine-grained locking and linearizable operations.
package waitlist

import (
	"errors"
	"sort"
	"sync"
)

// MaxCredits is the maximum number of credits a student may be enrolled in.
const MaxCredits = 21

type StudentID string
type SectionID string

// Ticket is a caller-provided unique positive integer. Smaller tickets have
// higher promotion priority.
type Ticket int64

var (
	ErrInvalidTicket   = errors.New("waitlist: ticket must be a positive integer")
	ErrDuplicateTicket = errors.New("waitlist: ticket already in use")
	ErrUnknownStudent  = errors.New("waitlist: unknown student")
	ErrUnknownSection  = errors.New("waitlist: unknown section")
)

// SectionSpec describes a teaching section at construction time.
type SectionSpec struct {
	ID       SectionID
	Capacity int
	Credits  int // 1..6
}

// StudentRequest is one waitlisted request of a student.
type StudentRequest struct {
	Ticket  Ticket
	Section SectionID
}

// WaitlistEntry is one entry in a section's waitlist, ordered by ticket.
type WaitlistEntry struct {
	Ticket  Ticket
	Student StudentID
}

// StudentView is a consistent view of one student.
type StudentView struct {
	Enrolled []SectionID      // sorted by section id
	Credits  int              // sum of credits of enrolled sections
	Waiting  []StudentRequest // waitlisted requests, sorted by ticket
}

// SectionView is a consistent view of one section.
type SectionView struct {
	Capacity int
	Credits  int
	Enrolled []StudentID     // sorted by student id
	Waitlist []WaitlistEntry // sorted by ticket
}

// FreeSeats returns the number of remaining seats.
func (v SectionView) FreeSeats() int { return v.Capacity - len(v.Enrolled) }

// Snapshot is a consistent, linearizable view of the requested entities.
type Snapshot struct {
	Students map[StudentID]StudentView
	Sections map[SectionID]SectionView
}

// studentState is the per-student mutable state, guarded by its own mutex.
type studentState struct {
	id       StudentID
	mu       sync.Mutex
	version  uint64
	credits  int
	enrolled map[SectionID]Ticket
	requests map[Ticket]SectionID // live waitlist requests
}

// sectionState is the per-section mutable state, guarded by its own mutex.
type sectionState struct {
	id       SectionID
	mu       sync.Mutex
	version  uint64
	capacity int
	credits  int
	enrolled map[StudentID]Ticket
	waitlist map[Ticket]StudentID
}

// ticketRef locates the request a ticket refers to.
type ticketRef struct {
	student StudentID
	section SectionID
}

// Engine is the waitlist component. It is safe for concurrent use.
type Engine struct {
	students map[StudentID]*studentState
	sections map[SectionID]*sectionState

	// registryMu only briefly protects the ticket registry; no enrollment,
	// cancellation or promotion work happens under it.
	registryMu sync.Mutex
	registry   map[Ticket]ticketRef
}

// New builds an Engine with the given students and sections. Initially no
// student is enrolled anywhere.
func New(students []StudentID, sections []SectionSpec) *Engine {
	e := &Engine{
		students: make(map[StudentID]*studentState, len(students)),
		sections: make(map[SectionID]*sectionState, len(sections)),
		registry: make(map[Ticket]ticketRef),
	}
	for _, id := range students {
		e.students[id] = &studentState{
			id:       id,
			enrolled: make(map[SectionID]Ticket),
			requests: make(map[Ticket]SectionID),
		}
	}
	for _, spec := range sections {
		e.sections[spec.ID] = &sectionState{
			id:       spec.ID,
			capacity: spec.Capacity,
			credits:  spec.Credits,
			enrolled: make(map[StudentID]Ticket),
			waitlist: make(map[Ticket]StudentID),
		}
	}
	return e
}

// Enqueue submits an enrollment request. The request joins the waitlist and
// promotion is attempted before Enqueue returns.
func (e *Engine) Enqueue(ticket Ticket, student StudentID, section SectionID) error {
	if ticket <= 0 {
		return ErrInvalidTicket
	}
	s, ok := e.students[student]
	if !ok {
		return ErrUnknownStudent
	}
	c, ok := e.sections[section]
	if !ok {
		return ErrUnknownSection
	}
	if !e.registerTicket(ticket, student, section) {
		return ErrDuplicateTicket
	}

	ls := newLockSet()
	ls.addStudent(s)
	ls.addSection(c)
	ls.lock()
	defer ls.unlock()

	if testHook != nil {
		testHook("enqueue", ls.studentIDs(), ls.sectionIDs())
	}

	s.requests[ticket] = section
	c.waitlist[ticket] = student
	s.bump()
	c.bump()

	// At any linearization point no other request can be promotable (every
	// operation completes its promotions before returning), and Enqueue only
	// consumes resources, so only the new request itself can become
	// promotable here.
	tryPromote(s, c, ticket)
	return nil
}

// registerTicket records a new ticket. It only touches the shared registry
// briefly and never performs enrollment work under registryMu.
func (e *Engine) registerTicket(t Ticket, student StudentID, section SectionID) bool {
	e.registryMu.Lock()
	defer e.registryMu.Unlock()
	if _, exists := e.registry[t]; exists {
		return false
	}
	e.registry[t] = ticketRef{student: student, section: section}
	return true
}

// lookupTicket finds the request a ticket refers to.
func (e *Engine) lookupTicket(t Ticket) (ticketRef, bool) {
	e.registryMu.Lock()
	defer e.registryMu.Unlock()
	ref, ok := e.registry[t]
	return ref, ok
}

// forgetTicket removes a ticket from the registry after its request was
// withdrawn.
func (e *Engine) forgetTicket(t Ticket) {
	e.registryMu.Lock()
	defer e.registryMu.Unlock()
	delete(e.registry, t)
}

func (s *studentState) bump() { s.version++ }
func (c *sectionState) bump() { c.version++ }

// tryPromote promotes the request (s, c, ticket) if a seat is free and the
// credit cap allows it. Callers must hold the locks of s and c.
func tryPromote(s *studentState, c *sectionState, ticket Ticket) {
	if len(c.enrolled) >= c.capacity {
		return
	}
	if s.credits+c.credits > MaxCredits {
		return
	}
	delete(s.requests, ticket)
	delete(c.waitlist, ticket)
	s.enrolled[c.id] = ticket
	c.enrolled[s.id] = ticket
	s.credits += c.credits
	s.bump()
	c.bump()
}

// Cancel withdraws a waitlisted or enrolled request. Cancelling an enrolled
// request frees its seat and credits and triggers promotion before returning.
// Repeated cancels have no effect.
func (e *Engine) Cancel(ticket Ticket) {
	ref, ok := e.lookupTicket(ticket)
	if !ok {
		return
	}
	s := e.students[ref.student]
	c := e.sections[ref.section]

	for {
		// Compute the closure of entities the cancellation and its
		// promotion cascade may touch: the student, everyone waiting for
		// the freed section, and every section the student waits for.
		// Reads are brief; the authoritative check happens after locking
		// via version numbers.
		s.mu.Lock()
		waitedSections := make([]SectionID, 0, len(s.requests))
		for _, sec := range s.requests {
			waitedSections = append(waitedSections, sec)
		}
		sVersion := s.version
		s.mu.Unlock()

		c.mu.Lock()
		waiters := make([]StudentID, 0, len(c.waitlist))
		for _, st := range c.waitlist {
			waiters = append(waiters, st)
		}
		cVersion := c.version
		c.mu.Unlock()

		ls := newLockSet()
		ls.addStudent(s)
		ls.addSection(c)
		for _, sec := range waitedSections {
			ls.addSection(e.sections[sec])
		}
		for _, st := range waiters {
			ls.addStudent(e.students[st])
		}
		ls.lock()

		// If the closure inputs changed while we were locking, restart
		// with a fresh closure.
		if s.version != sVersion || c.version != cVersion {
			ls.unlock()
			continue
		}

		if testHook != nil {
			testHook("cancel", ls.studentIDs(), ls.sectionIDs())
		}

		removed, freed := false, false
		if enrolledTicket, ok := s.enrolled[c.id]; ok && enrolledTicket == ticket {
			delete(s.enrolled, c.id)
			delete(c.enrolled, s.id)
			s.credits -= c.credits
			s.bump()
			c.bump()
			removed, freed = true, true
		} else if sec, ok := s.requests[ticket]; ok {
			sc := e.sections[sec]
			delete(s.requests, ticket)
			delete(sc.waitlist, ticket)
			s.bump()
			sc.bump()
			removed = true
		}

		if freed {
			e.cascade(s, c)
		}
		ls.unlock()

		if removed {
			e.forgetTicket(ticket)
		}
		return
	}
}

// cascade promotes waitlisted requests after a seat and credits were freed
// by cancelling (s, c). Only requests of s and requests waiting for c can
// have become promotable: promotions only consume seats and credits, never
// free them, so the candidate set is closed. The caller must hold the locks
// of s, c, every section s waits for, and every student waiting for c.
func (e *Engine) cascade(s *studentState, c *sectionState) {
	type candidate struct {
		ticket Ticket
		st     *studentState
		sec    *sectionState
	}
	byTicket := make(map[Ticket]candidate, len(s.requests)+len(c.waitlist))
	for t, secID := range s.requests {
		byTicket[t] = candidate{ticket: t, st: s, sec: e.sections[secID]}
	}
	for t, stID := range c.waitlist {
		if _, dup := byTicket[t]; !dup {
			byTicket[t] = candidate{ticket: t, st: e.students[stID], sec: c}
		}
	}
	ordered := make([]candidate, 0, len(byTicket))
	for _, cand := range byTicket {
		ordered = append(ordered, cand)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ticket < ordered[j].ticket })

	for _, cand := range ordered {
		// Resources only shrink during the cascade, so a request that is
		// not promotable now stays unpromotable for the rest of this
		// cascade and never blocks later eligible requests.
		tryPromote(cand.st, cand.sec, cand.ticket)
	}
}

// Inspect returns a consistent snapshot of the given students and sections.
func (e *Engine) Inspect(students []StudentID, sections []SectionID) Snapshot {
	ls := newLockSet()
	for _, id := range students {
		if s, ok := e.students[id]; ok {
			ls.addStudent(s)
		}
	}
	for _, id := range sections {
		if c, ok := e.sections[id]; ok {
			ls.addSection(c)
		}
	}
	ls.lock()
	defer ls.unlock()

	snap := Snapshot{
		Students: make(map[StudentID]StudentView, len(ls.students)),
		Sections: make(map[SectionID]SectionView, len(ls.sections)),
	}
	for _, s := range ls.students {
		view := StudentView{Credits: s.credits}
		for secID := range s.enrolled {
			view.Enrolled = append(view.Enrolled, secID)
		}
		sort.Slice(view.Enrolled, func(i, j int) bool { return view.Enrolled[i] < view.Enrolled[j] })
		for t, secID := range s.requests {
			view.Waiting = append(view.Waiting, StudentRequest{Ticket: t, Section: secID})
		}
		sort.Slice(view.Waiting, func(i, j int) bool { return view.Waiting[i].Ticket < view.Waiting[j].Ticket })
		snap.Students[s.id] = view
	}
	for _, c := range ls.sections {
		view := SectionView{Capacity: c.capacity, Credits: c.credits}
		for stID := range c.enrolled {
			view.Enrolled = append(view.Enrolled, stID)
		}
		sort.Slice(view.Enrolled, func(i, j int) bool { return view.Enrolled[i] < view.Enrolled[j] })
		for t, stID := range c.waitlist {
			view.Waitlist = append(view.Waitlist, WaitlistEntry{Ticket: t, Student: stID})
		}
		sort.Slice(view.Waitlist, func(i, j int) bool { return view.Waitlist[i].Ticket < view.Waitlist[j].Ticket })
		snap.Sections[c.id] = view
	}
	return snap
}

// testHook, when non-nil, is invoked while an operation holds its locks,
// before mutating any state. Tests use it to deterministically pause an
// operation inside its critical section. It must only be set while no
// operation is in flight.
var testHook func(op string, students []StudentID, sections []SectionID)

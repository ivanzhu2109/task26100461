package waitlist

import "sort"

// Model is a small sequential reference implementation of the waitlist
// semantics. Tests use it to check whether an observed concurrent history
// has a legal serial explanation. It is not safe for concurrent use.
type Model struct {
	secCapacity map[SectionID]int
	secCredits  map[SectionID]int
	credits     map[StudentID]int
	enrolled    map[StudentID]map[SectionID]Ticket
	secEnrolled map[SectionID]map[StudentID]Ticket
	// requests records every live request; enrolledFlag distinguishes
	// enrolled requests from waitlisted ones.
	requests map[Ticket]modelRequest
}

type modelRequest struct {
	student  StudentID
	section  SectionID
	enrolled bool
}

// NewModel builds a Model with the same initial state as New.
func NewModel(students []StudentID, sections []SectionSpec) *Model {
	m := &Model{
		secCapacity: make(map[SectionID]int, len(sections)),
		secCredits:  make(map[SectionID]int, len(sections)),
		credits:     make(map[StudentID]int, len(students)),
		enrolled:    make(map[StudentID]map[SectionID]Ticket, len(students)),
		secEnrolled: make(map[SectionID]map[StudentID]Ticket, len(sections)),
		requests:    make(map[Ticket]modelRequest),
	}
	for _, id := range students {
		m.enrolled[id] = make(map[SectionID]Ticket)
	}
	for _, spec := range sections {
		m.secCapacity[spec.ID] = spec.Capacity
		m.secCredits[spec.ID] = spec.Credits
		m.secEnrolled[spec.ID] = make(map[StudentID]Ticket)
	}
	return m
}

// Enqueue applies the sequential Enqueue semantics. It assumes the ticket is
// valid and unused, as the generated test histories guarantee.
func (m *Model) Enqueue(ticket Ticket, student StudentID, section SectionID) {
	m.requests[ticket] = modelRequest{student: student, section: section}
	m.cascade()
}

// Cancel applies the sequential Cancel semantics.
func (m *Model) Cancel(ticket Ticket) {
	req, ok := m.requests[ticket]
	if !ok {
		return
	}
	delete(m.requests, ticket)
	if req.enrolled {
		delete(m.enrolled[req.student], req.section)
		delete(m.secEnrolled[req.section], req.student)
		m.credits[req.student] -= m.secCredits[req.section]
		m.cascade()
	}
}

// cascade repeatedly promotes the smallest-ticket promotable waitlisted
// request until none remain.
func (m *Model) cascade() {
	for {
		best := Ticket(0)
		found := false
		for t, req := range m.requests {
			if req.enrolled {
				continue
			}
			if found && t >= best {
				continue
			}
			if !m.promotable(req) {
				continue
			}
			best, found = t, true
		}
		if !found {
			return
		}
		m.promote(best)
	}
}

func (m *Model) promotable(req modelRequest) bool {
	if len(m.secEnrolled[req.section]) >= m.secCapacity[req.section] {
		return false
	}
	return m.credits[req.student]+m.secCredits[req.section] <= MaxCredits
}

func (m *Model) promote(t Ticket) {
	req := m.requests[t]
	req.enrolled = true
	m.requests[t] = req
	m.enrolled[req.student][req.section] = t
	m.secEnrolled[req.section][req.student] = t
	m.credits[req.student] += m.secCredits[req.section]
}

// Inspect produces the same Snapshot shape as Engine.Inspect.
func (m *Model) Inspect(students []StudentID, sections []SectionID) Snapshot {
	snap := Snapshot{
		Students: make(map[StudentID]StudentView, len(students)),
		Sections: make(map[SectionID]SectionView, len(sections)),
	}
	for _, id := range students {
		view := StudentView{Credits: m.credits[id]}
		for secID := range m.enrolled[id] {
			view.Enrolled = append(view.Enrolled, secID)
		}
		sort.Slice(view.Enrolled, func(i, j int) bool { return view.Enrolled[i] < view.Enrolled[j] })
		for t, req := range m.requests {
			if req.student == id && !req.enrolled {
				view.Waiting = append(view.Waiting, StudentRequest{Ticket: t, Section: req.section})
			}
		}
		sort.Slice(view.Waiting, func(i, j int) bool { return view.Waiting[i].Ticket < view.Waiting[j].Ticket })
		snap.Students[id] = view
	}
	for _, id := range sections {
		view := SectionView{Capacity: m.secCapacity[id], Credits: m.secCredits[id]}
		for stID := range m.secEnrolled[id] {
			view.Enrolled = append(view.Enrolled, stID)
		}
		sort.Slice(view.Enrolled, func(i, j int) bool { return view.Enrolled[i] < view.Enrolled[j] })
		for t, req := range m.requests {
			if req.section == id && !req.enrolled {
				view.Waitlist = append(view.Waitlist, WaitlistEntry{Ticket: t, Student: req.student})
			}
		}
		sort.Slice(view.Waitlist, func(i, j int) bool { return view.Waitlist[i].Ticket < view.Waitlist[j].Ticket })
		snap.Sections[id] = view
	}
	return snap
}

// clone deep-copies the model so the linearizability checker can backtrack.
func (m *Model) clone() *Model {
	cp := &Model{
		secCapacity: make(map[SectionID]int, len(m.secCapacity)),
		secCredits:  make(map[SectionID]int, len(m.secCredits)),
		credits:     make(map[StudentID]int, len(m.credits)),
		enrolled:    make(map[StudentID]map[SectionID]Ticket, len(m.enrolled)),
		secEnrolled: make(map[SectionID]map[StudentID]Ticket, len(m.secEnrolled)),
		requests:    make(map[Ticket]modelRequest, len(m.requests)),
	}
	for k, v := range m.secCapacity {
		cp.secCapacity[k] = v
	}
	for k, v := range m.secCredits {
		cp.secCredits[k] = v
	}
	for k, v := range m.credits {
		cp.credits[k] = v
	}
	for k, v := range m.enrolled {
		inner := make(map[SectionID]Ticket, len(v))
		for kk, vv := range v {
			inner[kk] = vv
		}
		cp.enrolled[k] = inner
	}
	for k, v := range m.secEnrolled {
		inner := make(map[StudentID]Ticket, len(v))
		for kk, vv := range v {
			inner[kk] = vv
		}
		cp.secEnrolled[k] = inner
	}
	for k, v := range m.requests {
		cp.requests[k] = v
	}
	return cp
}

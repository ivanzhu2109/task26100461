package waitlist

import (
	"sort"
	"sync"
)

// entityID identifies a student or section node in the relation graph.
type entityID string

func studentEnt(name string) entityID { return entityID("S:" + name) }
func sectionEnt(name string) entityID { return entityID("X:" + name) }

// component guards every entity currently in one connected component of
// the student-section relation graph. Components merge but never split.
type component struct {
	id int64
	mu sync.Mutex
}

type studentState struct {
	name     string
	credits  int
	enrolled map[string]int64 // section name -> ticket
	waiting  map[string]int64 // section name -> ticket
}

type sectionState struct {
	name     string
	capacity int
	credits  int
	enrolled int
	requests map[int64]*request // all live requests, waiting or enrolled
}

type request struct {
	ticket   int64
	student  string
	section  string
	enrolled bool
}

// findLocked returns the root and its component for e.
// Caller must hold s.reg.
func (s *System) findLocked(e entityID) (entityID, *component) {
	root := e
	for s.parent[root] != root {
		root = s.parent[root]
	}
	for cur := e; s.parent[cur] != root; {
		next := s.parent[cur]
		s.parent[cur] = root
		cur = next
	}
	return root, s.comp[root]
}

// lockOne locks the component currently containing e and returns it.
func (s *System) lockOne(e entityID) *component {
	for {
		s.reg.Lock()
		_, c := s.findLocked(e)
		s.reg.Unlock()
		c.mu.Lock()
		s.reg.Lock()
		_, now := s.findLocked(e)
		s.reg.Unlock()
		if now == c {
			return c
		}
		c.mu.Unlock()
	}
}

// lockTwo locks the components of a and b, merging them if they differ,
// and returns the single locked component containing both.
func (s *System) lockTwo(a, b entityID) *component {
	for {
		s.reg.Lock()
		_, ca := s.findLocked(a)
		_, cb := s.findLocked(b)
		s.reg.Unlock()
		if ca == cb {
			ca.mu.Lock()
			s.reg.Lock()
			_, na := s.findLocked(a)
			_, nb := s.findLocked(b)
			s.reg.Unlock()
			if na == ca && nb == ca {
				return ca
			}
			ca.mu.Unlock()
			continue
		}
		first, second := ca, cb
		if cb.id < ca.id {
			first, second = cb, ca
		}
		first.mu.Lock()
		second.mu.Lock()
		s.reg.Lock()
		nra, nca := s.findLocked(a)
		nrb, ncb := s.findLocked(b)
		if nca == ca && ncb == cb {
			// Merge into the lower-id component for stable lock ordering;
			// the survivor is the component we return locked.
			if ca.id < cb.id {
				s.parent[nrb] = nra
				delete(s.comp, nrb)
			} else {
				s.parent[nra] = nrb
				delete(s.comp, nra)
			}
			s.reg.Unlock()
			second.mu.Unlock()
			return first
		}
		s.reg.Unlock()
		second.mu.Unlock()
		first.mu.Unlock()
	}
}

// lockAll locks the components of all given entities in component-id
// order and returns them (deduplicated, in lock order).
func (s *System) lockAll(entities []entityID) []*component {
	for {
		s.reg.Lock()
		set := map[*component]bool{}
		for _, e := range entities {
			_, c := s.findLocked(e)
			set[c] = true
		}
		s.reg.Unlock()
		comps := make([]*component, 0, len(set))
		for c := range set {
			comps = append(comps, c)
		}
		sort.Slice(comps, func(i, j int) bool { return comps[i].id < comps[j].id })
		for _, c := range comps {
			c.mu.Lock()
		}
		s.reg.Lock()
		ok := true
		for _, e := range entities {
			_, c := s.findLocked(e)
			if !set[c] {
				ok = false
				break
			}
		}
		s.reg.Unlock()
		if ok {
			return comps
		}
		for _, c := range comps {
			c.mu.Unlock()
		}
	}
}

func unlockAll(comps []*component) {
	for _, c := range comps {
		c.mu.Unlock()
	}
}

// promoteClosure repeatedly promotes the smallest-ticket promotable
// request among the given sections, until none is promotable. Because
// the system maintains the invariant that no request is promotable
// between operations, the sections touched by an operation are the only
// places where promotability can newly hold, so this is equivalent to
// the global smallest-ticket-first loop. Promotions only consume seats
// and add credits, so they never create promotability elsewhere.
// Caller must hold the lock of the component containing the sections.
func (s *System) promoteClosure(secs map[string]bool) {
	for {
		var best *request
		var bestSec *sectionState
		for name := range secs {
			sec := s.sections[name]
			if sec.enrolled >= sec.capacity {
				continue
			}
			for _, r := range sec.requests {
				if r.enrolled {
					continue
				}
				st := s.students[r.student]
				if st.credits+sec.credits > MaxCredits {
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
		bestSec.enrolled++
		st := s.students[best.student]
		st.credits += bestSec.credits
		st.enrolled[bestSec.name] = best.ticket
		delete(st.waiting, bestSec.name)
	}
}

package waitlist

import "sort"

// lockSet collects the per-entity mutexes an operation needs and acquires
// them in a fixed global order: students first (sorted by id), then sections
// (sorted by id). Because every operation uses the same order, concurrent
// operations on overlapping entity sets cannot deadlock, while operations on
// disjoint entity sets proceed fully in parallel.
type lockSet struct {
	studentSet map[StudentID]*studentState
	sectionSet map[SectionID]*sectionState
	students   []*studentState
	sections   []*sectionState
	locked     bool
}

func newLockSet() *lockSet {
	return &lockSet{
		studentSet: make(map[StudentID]*studentState),
		sectionSet: make(map[SectionID]*sectionState),
	}
}

func (ls *lockSet) addStudent(s *studentState) {
	if _, ok := ls.studentSet[s.id]; !ok {
		ls.studentSet[s.id] = s
		ls.students = append(ls.students, s)
	}
}

func (ls *lockSet) addSection(c *sectionState) {
	if _, ok := ls.sectionSet[c.id]; !ok {
		ls.sectionSet[c.id] = c
		ls.sections = append(ls.sections, c)
	}
}

func (ls *lockSet) lock() {
	sort.Slice(ls.students, func(i, j int) bool { return ls.students[i].id < ls.students[j].id })
	sort.Slice(ls.sections, func(i, j int) bool { return ls.sections[i].id < ls.sections[j].id })
	for _, s := range ls.students {
		s.mu.Lock()
	}
	for _, c := range ls.sections {
		c.mu.Lock()
	}
	ls.locked = true
}

func (ls *lockSet) unlock() {
	for _, c := range ls.sections {
		c.mu.Unlock()
	}
	for _, s := range ls.students {
		s.mu.Unlock()
	}
	ls.locked = false
}

// studentIDs returns the sorted ids of the locked students (for testHook).
func (ls *lockSet) studentIDs() []StudentID {
	ids := make([]StudentID, 0, len(ls.students))
	for _, s := range ls.students {
		ids = append(ids, s.id)
	}
	return ids
}

// sectionIDs returns the sorted ids of the locked sections (for testHook).
func (ls *lockSet) sectionIDs() []SectionID {
	ids := make([]SectionID, 0, len(ls.sections))
	for _, c := range ls.sections {
		ids = append(ids, c.id)
	}
	return ids
}

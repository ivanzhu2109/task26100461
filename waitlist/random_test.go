package waitlist

import (
	"math/rand"
	"testing"
)

// TestRandomHistoriesLinearizable fires short random concurrent histories at
// the engine and checks that each observed history — including every Inspect
// result — has a legal serial explanation under the reference model.
func TestRandomHistoriesLinearizable(t *testing.T) {
	students := []StudentID{"s0", "s1", "s2"}
	sections := []SectionSpec{
		{ID: "a", Capacity: 1, Credits: 1},
		{ID: "b", Capacity: 2, Credits: 2},
		{ID: "c", Capacity: 1, Credits: 3},
		{ID: "d", Capacity: 1, Credits: 6},
	}
	sectionIDs := []SectionID{"a", "b", "c", "d"}

	for round := 0; round < 5; round++ {
		rng := rand.New(rand.NewSource(int64(round) + 1))
		e := New(students, sections)
		r := newRecorder(e)

		// A shuffled mix of operation kinds; tickets are unique and
		// increasing in generation order.
		kinds := []opKind{opEnqueue, opEnqueue, opEnqueue, opEnqueue, opCancel, opCancel, opInspect, opInspect}
		rng.Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })

		// The input contract guarantees that a student never holds two live
		// requests for the same section, so draw distinct (student, section)
		// pairs for the enqueues of this round.
		var pairs []struct {
			st  StudentID
			sec SectionID
		}
		for _, st := range students {
			for _, sec := range sectionIDs {
				pairs = append(pairs, struct {
					st  StudentID
					sec SectionID
				}{st, sec})
			}
		}
		rng.Shuffle(len(pairs), func(i, j int) { pairs[i], pairs[j] = pairs[j], pairs[i] })
		nextPair := 0

		nextTicket := Ticket(1)
		var ops []func()
		for _, kind := range kinds {
			switch kind {
			case opEnqueue:
				ticket := nextTicket
				nextTicket++
				st, sec := pairs[nextPair].st, pairs[nextPair].sec
				nextPair++
				ops = append(ops, func() { r.Enqueue(ticket, st, sec) })
			case opCancel:
				ticket := Ticket(1 + rng.Intn(int(nextTicket))) // may not exist: fine
				ops = append(ops, func() { r.Cancel(ticket) })
			case opInspect:
				sts := []StudentID{students[rng.Intn(len(students))]}
				secs := []SectionID{sectionIDs[rng.Intn(len(sectionIDs))], sectionIDs[rng.Intn(len(sectionIDs))]}
				ops = append(ops, func() { r.Inspect(sts, secs) })
			}
		}
		runConcurrent(ops...)

		// A final full read that must linearize after everything else.
		r.Inspect(students, sectionIDs)

		checkLinearizable(t, NewModel(students, sections), r.ops)
	}
}

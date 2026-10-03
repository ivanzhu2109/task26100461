package waitlist

import (
	"sync"
	"testing"
)

func mustEnqueue(t *testing.T, r *recorder, ticket Ticket, student StudentID, section SectionID) {
	t.Helper()
	if err := r.Enqueue(ticket, student, section); err != nil {
		t.Fatalf("Enqueue(%d, %s, %s): %v", ticket, student, section, err)
	}
}

// runConcurrent starts every op at a common barrier and waits for all to
// finish.
func runConcurrent(ops ...func()) {
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	for _, op := range ops {
		wg.Add(1)
		go func(op func()) {
			defer wg.Done()
			<-barrier
			op()
		}(op)
	}
	close(barrier)
	wg.Wait()
}

func TestBasicEnqueuePromoteCancel(t *testing.T) {
	e := New(
		[]StudentID{"s1", "s2"},
		[]SectionSpec{{ID: "a", Capacity: 1, Credits: 3}},
	)
	r := newRecorder(e)

	mustEnqueue(t, r, 1, "s1", "a") // promoted immediately
	mustEnqueue(t, r, 2, "s2", "a") // waits: no seats

	snap := r.Inspect([]StudentID{"s1", "s2"}, []SectionID{"a"})
	if got := snap.Students["s1"].Credits; got != 3 {
		t.Fatalf("s1 credits = %d, want 3", got)
	}
	if got := snap.Sections["a"].FreeSeats(); got != 0 {
		t.Fatalf("free seats = %d, want 0", got)
	}
	if got := snap.Sections["a"].Waitlist; len(got) != 1 || got[0].Student != "s2" {
		t.Fatalf("waitlist = %v, want [s2]", got)
	}

	r.Cancel(1) // frees the seat; s2 must be promoted before Cancel returns
	snap = r.Inspect([]StudentID{"s1", "s2"}, []SectionID{"a"})
	if got := snap.Students["s2"].Credits; got != 3 {
		t.Fatalf("s2 credits = %d, want 3", got)
	}
	if got := snap.Sections["a"].Enrolled; len(got) != 1 || got[0] != "s2" {
		t.Fatalf("enrolled = %v, want [s2]", got)
	}

	r.Cancel(1) // repeated cancel: no effect
	r.Cancel(2)
	snap = r.Inspect([]StudentID{"s1", "s2"}, []SectionID{"a"})
	if got := snap.Sections["a"].FreeSeats(); got != 1 {
		t.Fatalf("free seats after cancels = %d, want 1", got)
	}

	checkLinearizable(t, NewModel(
		[]StudentID{"s1", "s2"},
		[]SectionSpec{{ID: "a", Capacity: 1, Credits: 3}},
	), r.ops)
}

func TestEnqueueValidation(t *testing.T) {
	e := New([]StudentID{"s1"}, []SectionSpec{{ID: "a", Capacity: 1, Credits: 3}})
	if err := e.Enqueue(0, "s1", "a"); err != ErrInvalidTicket {
		t.Fatalf("ticket 0: %v", err)
	}
	if err := e.Enqueue(-3, "s1", "a"); err != ErrInvalidTicket {
		t.Fatalf("negative ticket: %v", err)
	}
	if err := e.Enqueue(1, "ghost", "a"); err != ErrUnknownStudent {
		t.Fatalf("unknown student: %v", err)
	}
	if err := e.Enqueue(1, "s1", "ghost"); err != ErrUnknownSection {
		t.Fatalf("unknown section: %v", err)
	}
	if err := e.Enqueue(1, "s1", "a"); err != nil {
		t.Fatalf("valid enqueue: %v", err)
	}
	if err := e.Enqueue(1, "s1", "a"); err != ErrDuplicateTicket {
		t.Fatalf("duplicate ticket: %v", err)
	}
}

// TestConcurrentCreditCap: a student with 18 credits concurrently requests
// two 3-credit sections while reads happen. At most one request may be
// promoted, and every observed read must fit one legal serial order.
func TestConcurrentCreditCap(t *testing.T) {
	students := []StudentID{"s"}
	sections := []SectionSpec{
		{ID: "p1", Capacity: 1, Credits: 6},
		{ID: "p2", Capacity: 1, Credits: 6},
		{ID: "p3", Capacity: 1, Credits: 6},
		{ID: "a", Capacity: 1, Credits: 3},
		{ID: "b", Capacity: 1, Credits: 3},
	}
	e := New(students, sections)
	r := newRecorder(e)

	mustEnqueue(t, r, 1, "s", "p1")
	mustEnqueue(t, r, 2, "s", "p2")
	mustEnqueue(t, r, 3, "s", "p3") // 18 credits

	ops := []func(){
		func() { mustEnqueue(t, r, 4, "s", "a") },
		func() { mustEnqueue(t, r, 5, "s", "b") },
		func() { r.Inspect([]StudentID{"s"}, []SectionID{"a", "b"}) },
		func() { r.Inspect([]StudentID{"s"}, []SectionID{"a", "b"}) },
		func() { r.Inspect([]StudentID{"s"}, []SectionID{"a", "b"}) },
	}
	runConcurrent(ops...)

	snap := r.Inspect([]StudentID{"s"}, []SectionID{"a", "b"})
	enrolled := 0
	for _, sec := range []SectionID{"a", "b"} {
		for _, st := range snap.Sections[sec].Enrolled {
			if st == "s" {
				enrolled++
			}
		}
	}
	if enrolled != 1 {
		t.Fatalf("s enrolled in %d of a/b, want exactly 1 (18+3+3 would exceed 21)", enrolled)
	}
	if got := snap.Students["s"].Credits; got != 21 {
		t.Fatalf("s credits = %d, want 21", got)
	}

	checkLinearizable(t, NewModel(students, sections), r.ops)
}

// TestPromotionPriorityByTicket: a 21-credit student waits in two sections
// with free seats. Dropping a 3-credit course must promote the smaller
// ticket first; the remaining request still exceeds the cap and keeps
// waiting.
func TestPromotionPriorityByTicket(t *testing.T) {
	students := []StudentID{"s"}
	sections := []SectionSpec{
		{ID: "c1", Capacity: 1, Credits: 6},
		{ID: "c2", Capacity: 1, Credits: 6},
		{ID: "c3", Capacity: 1, Credits: 6},
		{ID: "c4", Capacity: 1, Credits: 3},
		{ID: "x", Capacity: 1, Credits: 3},
		{ID: "y", Capacity: 1, Credits: 3},
	}
	e := New(students, sections)
	r := newRecorder(e)

	mustEnqueue(t, r, 1, "s", "c1")
	mustEnqueue(t, r, 2, "s", "c2")
	mustEnqueue(t, r, 3, "s", "c3")
	mustEnqueue(t, r, 4, "s", "c4") // 21 credits
	mustEnqueue(t, r, 10, "s", "x") // waits: 21+3 > 21
	mustEnqueue(t, r, 20, "s", "y") // waits: 21+3 > 21

	r.Cancel(4) // 18 credits; ticket 10 must win over ticket 20

	snap := r.Inspect([]StudentID{"s"}, []SectionID{"x", "y"})
	if got := snap.Sections["x"].Enrolled; len(got) != 1 || got[0] != "s" {
		t.Fatalf("x enrolled = %v, want [s]", got)
	}
	if got := snap.Sections["y"].Waitlist; len(got) != 1 || got[0].Ticket != 20 {
		t.Fatalf("y waitlist = %v, want [ticket 20]", got)
	}
	if got := snap.Students["s"].Credits; got != 21 {
		t.Fatalf("s credits = %d, want 21", got)
	}

	checkLinearizable(t, NewModel(students, sections), r.ops)
}

// TestLowTicketOverCreditDoesNotBlock: when the smallest-ticket waiter
// exceeds the credit cap, a later eligible request still gets the freed
// seat. A second phase interleaves cancels, enqueues and reads.
func TestLowTicketOverCreditDoesNotBlock(t *testing.T) {
	students := []StudentID{"p", "q", "r"}
	sections := []SectionSpec{
		{ID: "d1", Capacity: 1, Credits: 6},
		{ID: "d2", Capacity: 1, Credits: 6},
		{ID: "d3", Capacity: 1, Credits: 6},
		{ID: "d4", Capacity: 1, Credits: 1},
		{ID: "z", Capacity: 1, Credits: 3},
		{ID: "w", Capacity: 1, Credits: 2},
	}
	e := New(students, sections)
	r := newRecorder(e)

	mustEnqueue(t, r, 1, "r", "z")  // r holds the only seat of z
	mustEnqueue(t, r, 2, "p", "d1") // p reaches 19 credits
	mustEnqueue(t, r, 3, "p", "d2")
	mustEnqueue(t, r, 4, "p", "d3")
	mustEnqueue(t, r, 5, "p", "d4")
	mustEnqueue(t, r, 6, "p", "z") // waits: 19+3 > 21
	mustEnqueue(t, r, 7, "q", "z") // waits: no seats

	r.Cancel(1) // frees z's seat; p (ticket 6) cannot take it, q (ticket 7) can

	snap := r.Inspect([]StudentID{"p", "q"}, []SectionID{"z"})
	if got := snap.Sections["z"].Enrolled; len(got) != 1 || got[0] != "q" {
		t.Fatalf("z enrolled = %v, want [q]", got)
	}
	if got := snap.Sections["z"].Waitlist; len(got) != 1 || got[0].Student != "p" {
		t.Fatalf("z waitlist = %v, want [p]", got)
	}

	// Interleaved phase: cancel p's wait, enqueue into w, and read, all at
	// once. Every observation must fit one legal serial order.
	ops := []func(){
		func() { r.Cancel(6) },
		func() { mustEnqueue(t, r, 8, "q", "w") },
		func() { mustEnqueue(t, r, 9, "p", "w") },
		func() { r.Cancel(7) },
		func() { r.Inspect([]StudentID{"p", "q"}, []SectionID{"z", "w"}) },
		func() { r.Inspect([]StudentID{"p", "q"}, []SectionID{"z", "w"}) },
	}
	runConcurrent(ops...)

	checkLinearizable(t, NewModel(students, sections), r.ops)
}

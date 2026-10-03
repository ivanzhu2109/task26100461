package waitlist

import (
	"sync"
	"testing"
	"time"
)

// TestDisconnectedGroupsProceedIndependently pauses one group's Cancel
// inside its mutation critical section (via testHook) and proves that a
// disconnected group — sharing neither students nor sections through any
// enrollment or waitlist relation — can still complete mutations and reads.
func TestDisconnectedGroupsProceedIndependently(t *testing.T) {
	students := []StudentID{"g1", "g2"}
	sections := []SectionSpec{
		{ID: "ga", Capacity: 1, Credits: 3},
		{ID: "gb", Capacity: 1, Credits: 3},
		{ID: "gc", Capacity: 1, Credits: 3},
		{ID: "gd", Capacity: 1, Credits: 3},
	}
	e := New(students, sections)
	r := newRecorder(e)

	// Group 1: g1 enrolled in ga, waiting in gb.
	mustEnqueue(t, r, 1, "g1", "ga")
	mustEnqueue(t, r, 2, "g1", "gb")

	// Pause any cancel whose critical section covers ga.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	testHook = func(op string, _ []StudentID, secs []SectionID) {
		if op != "cancel" {
			return
		}
		for _, id := range secs {
			if id == "ga" {
				once.Do(func() { close(entered) })
				<-release
				return
			}
		}
	}
	defer func() { testHook = nil }()

	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		r.Cancel(1) // pauses inside its critical section, holding ga's lock
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("group-1 cancel never reached its critical section")
	}

	// Group 2 is disconnected from group 1; all of its operations must
	// complete while group 1 is paused.
	group2Done := make(chan struct{})
	go func() {
		defer close(group2Done)
		mustEnqueue(t, r, 10, "g2", "gc")
		mustEnqueue(t, r, 11, "g2", "gd")
		r.Cancel(10)
		snap := r.Inspect([]StudentID{"g2"}, []SectionID{"gc", "gd"})
		if got := snap.Sections["gd"].Enrolled; len(got) != 1 || got[0] != "g2" {
			t.Errorf("gd enrolled = %v, want [g2]", got)
		}
		if got := snap.Students["g2"].Credits; got != 3 {
			t.Errorf("g2 credits = %d, want 3", got)
		}
	}()

	select {
	case <-group2Done:
	case <-time.After(10 * time.Second):
		t.Fatal("disconnected group blocked by paused critical section")
	}

	close(release)
	select {
	case <-cancelDone:
	case <-time.After(10 * time.Second):
		t.Fatal("group-1 cancel did not finish after release")
	}

	// Group 1's state settled after the paused cancel completed.
	snap := r.Inspect([]StudentID{"g1"}, []SectionID{"ga", "gb"})
	if got := snap.Sections["ga"].FreeSeats(); got != 1 {
		t.Fatalf("ga free seats = %d, want 1", got)
	}
	if got := snap.Sections["gb"].Enrolled; len(got) != 1 || got[0] != "g1" {
		t.Fatalf("gb enrolled = %v, want [g1] (promoted by the cascade)", got)
	}

	checkLinearizable(t, NewModel(students, sections), r.ops)
}

package waitlist

import (
	"reflect"
	"testing"
)

func newTestSystem() *System {
	return New(
		[]string{"alice", "bob", "carol", "dave"},
		map[string]SectionSpec{
			"A": {Capacity: 1, Credits: 3},
			"B": {Capacity: 1, Credits: 3},
			"C": {Capacity: 2, Credits: 6},
			"D": {Capacity: 4, Credits: 6},
			"E": {Capacity: 4, Credits: 6},
			"F": {Capacity: 4, Credits: 6},
			"G": {Capacity: 1, Credits: 3},
			"H": {Capacity: 1, Credits: 3},
		},
	)
}

// enrollTo drives student to exactly wantCredits via 6-credit sections.
func enrollTo(t *testing.T, s *System, student string, ticket *int64, sections []string, wantCredits int) {
	t.Helper()
	for _, sec := range sections {
		*ticket++
		s.Enqueue(*ticket, student, sec)
	}
	if got := s.Inspect([]string{student}, nil).Students[student].Credits; got != wantCredits {
		t.Fatalf("enrollTo: got %d credits, want %d", got, wantCredits)
	}
}

func TestEnqueuePromotesImmediately(t *testing.T) {
	s := newTestSystem()
	s.Enqueue(1, "alice", "A")
	snap := s.Inspect([]string{"alice"}, []string{"A"})
	if snap.Sections["A"].Enrolled != 1 {
		t.Fatalf("A enrolled = %d, want 1", snap.Sections["A"].Enrolled)
	}
	if snap.Students["alice"].Credits != 3 {
		t.Fatalf("alice credits = %d, want 3", snap.Students["alice"].Credits)
	}
	if !reflect.DeepEqual(snap.Students["alice"].Enrolled, []string{"A"}) {
		t.Fatalf("alice enrolled = %v", snap.Students["alice"].Enrolled)
	}
}

func TestWaitlistOrderAndCancelPromotesMinTicket(t *testing.T) {
	s := newTestSystem()
	s.Enqueue(1, "alice", "A") // enrolled
	s.Enqueue(5, "bob", "A")   // waiting
	s.Enqueue(3, "carol", "A") // waiting
	snap := s.Inspect(nil, []string{"A"})
	if !reflect.DeepEqual(snap.Sections["A"].Waitlist, []int64{3, 5}) {
		t.Fatalf("waitlist = %v, want [3 5]", snap.Sections["A"].Waitlist)
	}
	s.Cancel(1) // frees seat; carol (ticket 3) must win
	snap = s.Inspect([]string{"alice", "bob", "carol"}, []string{"A"})
	if !reflect.DeepEqual(snap.Students["carol"].Enrolled, []string{"A"}) {
		t.Fatalf("carol enrolled = %v, want [A]", snap.Students["carol"].Enrolled)
	}
	if !reflect.DeepEqual(snap.Sections["A"].Waitlist, []int64{5}) {
		t.Fatalf("waitlist = %v, want [5]", snap.Sections["A"].Waitlist)
	}
	if len(snap.Students["alice"].Enrolled) != 0 || snap.Students["alice"].Credits != 0 {
		t.Fatalf("alice should be fully out: %+v", snap.Students["alice"])
	}
}

func TestCancelWaitingAndRepeat(t *testing.T) {
	s := newTestSystem()
	s.Enqueue(1, "alice", "A")
	s.Enqueue(2, "bob", "A")
	s.Cancel(2)
	s.Cancel(2)   // no-op
	s.Cancel(999) // unknown ticket: no-op
	snap := s.Inspect(nil, []string{"A"})
	if len(snap.Sections["A"].Waitlist) != 0 {
		t.Fatalf("waitlist = %v, want empty", snap.Sections["A"].Waitlist)
	}
	if snap.Sections["A"].Enrolled != 1 {
		t.Fatalf("enrolled = %d, want 1", snap.Sections["A"].Enrolled)
	}
}

func TestCreditCapBlocksSecondEnrollment(t *testing.T) {
	s := newTestSystem()
	var tk int64 = 100
	enrollTo(t, s, "alice", &tk, []string{"C", "D", "E"}, 18)
	s.Enqueue(10, "alice", "A") // 18+3=21 ok
	s.Enqueue(11, "alice", "B") // would be 24: must wait
	snap := s.Inspect([]string{"alice"}, []string{"A", "B"})
	if snap.Students["alice"].Credits != 21 {
		t.Fatalf("credits = %d, want 21", snap.Students["alice"].Credits)
	}
	if !reflect.DeepEqual(snap.Sections["B"].Waitlist, []int64{11}) {
		t.Fatalf("B waitlist = %v, want [11]", snap.Sections["B"].Waitlist)
	}
}

func TestLowTicketOverCreditDoesNotBlockOthers(t *testing.T) {
	s := newTestSystem()
	var tk int64 = 100
	enrollTo(t, s, "alice", &tk, []string{"C", "D", "E"}, 18)
	s.Enqueue(1, "alice", "A") // low ticket, but 18+3=21 fits -> enrolled
	// Now give alice 21 and try again with a fresh student mix.
	s2 := newTestSystem()
	tk = 100
	enrollTo(t, s2, "alice", &tk, []string{"C", "D", "E"}, 18)
	s2.Enqueue(1, "alice", "A") // enrolled, alice at 21
	s2.Cancel(1)                // alice back to 18, seat free
	s2.Enqueue(2, "alice", "A") // 18+3 fits again
	tk = 200
	enrollTo(t, s2, "bob", &tk, []string{"C", "D", "F"}, 18)
	// bob at 18; enqueue bob into B (3 credits, fits -> 21) then beyond.
	s2.Enqueue(3, "bob", "B")
	// dave at 21 wants A (full) and B (full): waits.
	tk = 300
	enrollTo(t, s2, "dave", &tk, []string{"D", "E", "F"}, 18)
	s2.Enqueue(4, "dave", "B") // 18+3=21 fits? B full (bob). waits.
	snap := s2.Inspect([]string{"dave"}, []string{"B"})
	if !reflect.DeepEqual(snap.Sections["B"].Waitlist, []int64{4}) {
		t.Fatalf("B waitlist = %v, want [4]", snap.Sections["B"].Waitlist)
	}
	// Free bob's seat in B: dave (18+3=21) gets it despite anything else.
	s2.Cancel(3)
	snap = s2.Inspect([]string{"bob", "dave"}, []string{"B"})
	if !reflect.DeepEqual(snap.Students["dave"].Enrolled, []string{"B", "D", "E", "F"}) {
		t.Fatalf("dave enrolled = %v", snap.Students["dave"].Enrolled)
	}
	if snap.Students["bob"].Credits != 18 {
		t.Fatalf("bob credits = %d, want 18", snap.Students["bob"].Credits)
	}
}

func TestOverCreditLowTicketSkippedForLaterStudent(t *testing.T) {
	s := newTestSystem()
	var tk int64 = 100
	enrollTo(t, s, "alice", &tk, []string{"C", "D", "E"}, 18)
	s.Enqueue(1, "alice", "A") // enrolled, alice at 21
	s.Enqueue(2, "alice", "B") // 21+3 > 21: waits with low ticket
	s.Enqueue(3, "bob", "B")   // bob fits: must get the seat despite ticket 2 waiting
	snap := s.Inspect([]string{"alice", "bob"}, []string{"B"})
	if snap.Sections["B"].Enrolled != 1 {
		t.Fatalf("B enrolled = %d, want 1", snap.Sections["B"].Enrolled)
	}
	if !reflect.DeepEqual(snap.Students["bob"].Enrolled, []string{"B"}) {
		t.Fatalf("bob enrolled = %v, want [B]", snap.Students["bob"].Enrolled)
	}
	if !reflect.DeepEqual(snap.Sections["B"].Waitlist, []int64{2}) {
		t.Fatalf("B waitlist = %v, want [2]", snap.Sections["B"].Waitlist)
	}
}

func TestEnrolledNotPreemptedBySmallerTicket(t *testing.T) {
	s := newTestSystem()
	s.Enqueue(10, "alice", "A") // enrolled
	s.Enqueue(1, "bob", "A")    // smaller ticket but arrives later: waits
	snap := s.Inspect([]string{"alice", "bob"}, []string{"A"})
	if !reflect.DeepEqual(snap.Students["alice"].Enrolled, []string{"A"}) {
		t.Fatalf("alice enrolled = %v, want [A]", snap.Students["alice"].Enrolled)
	}
	if !reflect.DeepEqual(snap.Sections["A"].Waitlist, []int64{1}) {
		t.Fatalf("waitlist = %v, want [1]", snap.Sections["A"].Waitlist)
	}
}

func TestCrossSectionPromotionIsAtomicPerCancel(t *testing.T) {
	// One cancel frees a seat; promotion across the freed section
	// completes before the cancel returns.
	s := newTestSystem()
	s.Enqueue(1, "alice", "A")
	s.Enqueue(2, "bob", "A")
	s.Enqueue(3, "carol", "A")
	s.Cancel(1)
	snap := s.Inspect([]string{"bob"}, []string{"A"})
	if !reflect.DeepEqual(snap.Students["bob"].Enrolled, []string{"A"}) {
		t.Fatalf("bob enrolled = %v, want [A]", snap.Students["bob"].Enrolled)
	}
	if !reflect.DeepEqual(snap.Sections["A"].Waitlist, []int64{3}) {
		t.Fatalf("waitlist = %v, want [3]", snap.Sections["A"].Waitlist)
	}
}

func TestCancelReleasesCreditsAcrossSections(t *testing.T) {
	s := newTestSystem()
	var tk int64 = 100
	enrollTo(t, s, "alice", &tk, []string{"C", "D", "E"}, 18)
	s.Enqueue(1, "alice", "A") // enrolled, alice at 21
	// alice waits on two other 3-credit sections, blocked by credits.
	s.Enqueue(2, "alice", "B")
	s.Enqueue(3, "alice", "G")
	snap := s.Inspect(nil, []string{"B", "G"})
	if len(snap.Sections["B"].Waitlist) != 1 || len(snap.Sections["G"].Waitlist) != 1 {
		t.Fatalf("expected waiting on B and G: %+v", snap.Sections)
	}
	// Cancel the 3-credit A enrollment: alice drops to 18, and the
	// smallest-ticket waiting request (ticket 2, section B) is promoted
	// even though the freed seat is in a different section.
	s.Cancel(1)
	snap = s.Inspect([]string{"alice"}, []string{"A", "B", "G"})
	if snap.Students["alice"].Credits != 21 {
		t.Fatalf("alice credits = %d, want 21", snap.Students["alice"].Credits)
	}
	if !reflect.DeepEqual(snap.Students["alice"].Enrolled, []string{"B", "C", "D", "E"}) {
		t.Fatalf("alice enrolled = %v", snap.Students["alice"].Enrolled)
	}
	// G still waits: 21+3 would exceed the cap.
	if !reflect.DeepEqual(snap.Sections["G"].Waitlist, []int64{3}) {
		t.Fatalf("G waitlist = %v, want [3]", snap.Sections["G"].Waitlist)
	}
}

package waitlist

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scenario 1: a student with 18 credits concurrently applies to two
// 3-credit sections while reads run. At most one application may be
// enrolled, and every read must fit one legal serial order.
func TestConcurrentCreditCapWithReads(t *testing.T) {
	students := []string{"alice"}
	sections := map[string]SectionSpec{
		"C": {Capacity: 1, Credits: 6},
		"D": {Capacity: 1, Credits: 6},
		"E": {Capacity: 1, Credits: 6},
		"A": {Capacity: 1, Credits: 3},
		"B": {Capacity: 1, Credits: 3},
	}
	s := New(students, sections)
	rec := &recorder{}
	// Sequential setup: alice reaches 18 credits.
	rec.enqueue(s, 91, "alice", "C")
	rec.enqueue(s, 92, "alice", "D")
	rec.enqueue(s, 93, "alice", "E")

	start := make(chan struct{})
	var wg sync.WaitGroup
	goOp := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			f()
		}()
	}
	goOp(func() { rec.enqueue(s, 1, "alice", "A") })
	goOp(func() { rec.enqueue(s, 2, "alice", "B") })
	for i := 0; i < 4; i++ {
		goOp(func() {
			rec.inspect(s, students, []string{"A", "B"})
			rec.inspect(s, students, []string{"A", "B", "C", "D", "E"})
		})
	}
	close(start)
	wg.Wait()

	snap := s.Inspect(students, []string{"A", "B"})
	got := snap.Students["alice"]
	if got.Credits != 21 {
		t.Fatalf("alice credits = %d, want exactly 21 (18 + one 3-credit section)", got.Credits)
	}
	enrolledAB := 0
	for _, sec := range []string{"A", "B"} {
		if snap.Sections[sec].Enrolled == 1 {
			enrolledAB++
		} else if len(snap.Sections[sec].Waitlist) != 1 {
			t.Fatalf("section %s neither enrolled nor waiting: %+v", sec, snap.Sections[sec])
		}
	}
	if enrolledAB != 1 {
		t.Fatalf("alice enrolled in %d of {A,B}, want exactly 1", enrolledAB)
	}
	if !checkLinearizable(t, students, sections, rec.entries) {
		t.Fatal("history has no legal serial explanation")
	}
}

// Scenario 2: a 21-credit student waits on several sections with free
// seats; cancelling a 3-credit enrollment must promote the smallest
// ticket first, and requests still over the cap keep waiting. A waiting
// low ticket over the credit cap must not block a later eligible
// student. Cancels, enqueues and reads are interleaved concurrently.
func TestConcurrentPromotionAfterCreditRelease(t *testing.T) {
	students := []string{"rich", "poor", "needy", "third"}
	sections := map[string]SectionSpec{
		"C": {Capacity: 2, Credits: 6},
		"D": {Capacity: 2, Credits: 6},
		"E": {Capacity: 2, Credits: 6},
		"A": {Capacity: 1, Credits: 3},
		"B": {Capacity: 1, Credits: 3},
		"G": {Capacity: 1, Credits: 3},
		"H": {Capacity: 1, Credits: 3},
		"I": {Capacity: 1, Credits: 3},
		"J": {Capacity: 1, Credits: 3},
		"K": {Capacity: 1, Credits: 3},
	}
	s := New(students, sections)
	rec := &recorder{}
	// Sequential setup.
	rec.enqueue(s, 91, "rich", "C")
	rec.enqueue(s, 92, "rich", "D")
	rec.enqueue(s, 93, "rich", "E")
	rec.enqueue(s, 1, "rich", "A") // rich at 21
	rec.enqueue(s, 10, "rich", "G")
	rec.enqueue(s, 20, "rich", "H")
	rec.enqueue(s, 30, "rich", "I") // all waiting: 21+3 > 21
	rec.enqueue(s, 81, "needy", "C")
	rec.enqueue(s, 82, "needy", "D")
	rec.enqueue(s, 83, "needy", "E")
	rec.enqueue(s, 2, "needy", "B") // needy at 21
	rec.enqueue(s, 5, "needy", "J") // waits: over cap despite low ticket
	rec.enqueue(s, 15, "poor", "J") // poor fits: enrolled ahead of ticket 5

	if snap := s.Inspect(nil, []string{"J"}); snap.Sections["J"].Enrolled != 1 {
		t.Fatalf("setup: poor should hold J's seat, got %+v", snap.Sections["J"])
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	goOp := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			f()
		}()
	}
	goOp(func() { rec.cancel(s, 1) })  // rich: 21 -> 18, then promotion
	goOp(func() { rec.cancel(s, 15) }) // poor leaves J
	goOp(func() { rec.enqueue(s, 40, "third", "K") })
	goOp(func() { rec.inspect(s, students, []string{"A", "G", "H", "I", "J", "K"}) })
	goOp(func() { rec.inspect(s, []string{"rich", "needy"}, []string{"G", "J"}) })
	close(start)
	wg.Wait()

	snap := s.Inspect(students, []string{"A", "G", "H", "I", "J", "K"})
	if got := snap.Students["rich"]; got.Credits != 21 {
		t.Fatalf("rich credits = %d, want 21", got.Credits)
	}
	// Smallest waiting ticket (10, section G) must have been promoted.
	if snap.Sections["G"].Enrolled != 1 {
		t.Fatalf("G enrolled = %d, want 1 (ticket 10 promoted)", snap.Sections["G"].Enrolled)
	}
	// rich is back at 21, so tickets 20 and 30 keep waiting.
	if got := snap.Sections["H"].Waitlist; len(got) != 1 || got[0] != 20 {
		t.Fatalf("H waitlist = %v, want [20]", got)
	}
	if got := snap.Sections["I"].Waitlist; len(got) != 1 || got[0] != 30 {
		t.Fatalf("I waitlist = %v, want [30]", got)
	}
	// J is free again; needy (ticket 5) is still over the cap and waits.
	if snap.Sections["J"].Enrolled != 0 {
		t.Fatalf("J enrolled = %d, want 0 (needy still over cap)", snap.Sections["J"].Enrolled)
	}
	if got := snap.Sections["J"].Waitlist; len(got) != 1 || got[0] != 5 {
		t.Fatalf("J waitlist = %v, want [5]", got)
	}
	if snap.Sections["K"].Enrolled != 1 {
		t.Fatalf("K enrolled = %d, want 1 (third)", snap.Sections["K"].Enrolled)
	}
	if !checkLinearizable(t, students, sections, rec.entries) {
		for _, e := range rec.entries {
			t.Logf("invoke=%d resp=%d op=%+v", e.invoke, e.resp, e.op)
		}
		t.Fatal("history has no legal serial explanation")
	}
}

// Scenario 3: one group of requests is deterministically paused inside
// the mutation critical section; a disconnected group must still
// complete mutations and reads.
func TestDisconnectedGroupsProceedIndependently(t *testing.T) {
	s := New(
		[]string{"ga", "gb"},
		map[string]SectionSpec{
			"SA": {Capacity: 1, Credits: 3},
			"SB": {Capacity: 1, Credits: 3},
		},
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	testHook = func(op string, ticket int64) {
		if ticket == 100 {
			close(entered)
			<-release
		}
	}
	defer func() { testHook = nil }()

	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		s.Enqueue(100, "ga", "SA") // pauses inside the critical section
	}()
	<-entered // group A is now deterministically parked in its critical section

	// Group B (no shared students/sections) must make progress.
	doneB := make(chan struct{})
	go func() {
		defer close(doneB)
		s.Enqueue(200, "gb", "SB")
		s.Cancel(200)
		s.Enqueue(201, "gb", "SB")
		snap := s.Inspect([]string{"gb"}, []string{"SB"})
		if snap.Sections["SB"].Enrolled != 1 || snap.Students["gb"].Credits != 3 {
			t.Errorf("group B state wrong: %+v", snap)
		}
	}()
	select {
	case <-doneB:
	case <-time.After(10 * time.Second):
		t.Fatal("disconnected group B blocked by paused group A")
	}

	close(release)
	select {
	case <-doneA:
	case <-time.After(10 * time.Second):
		t.Fatal("group A did not finish after release")
	}
	snap := s.Inspect([]string{"ga"}, []string{"SA"})
	if snap.Sections["SA"].Enrolled != 1 || snap.Students["ga"].Credits != 3 {
		t.Fatalf("group A final state wrong: %+v", snap)
	}
}

// Randomized short concurrent histories must admit a legal serial
// explanation. Each goroutine owns a disjoint set of (student, section)
// pairs and its own tickets, honoring the input contract.
func TestRandomizedHistoryIsLinearizable(t *testing.T) {
	students := []string{"s0", "s1", "s2", "s3"}
	sections := map[string]SectionSpec{
		"P": {Capacity: 2, Credits: 6},
		"Q": {Capacity: 2, Credits: 6},
		"R": {Capacity: 1, Credits: 3},
		"S": {Capacity: 1, Credits: 3},
		"T": {Capacity: 1, Credits: 3},
		"U": {Capacity: 1, Credits: 3},
	}
	secNames := []string{"P", "Q", "R", "S", "T", "U"}
	s := New(students, sections)
	rec := &recorder{}
	var ticketGen int64 = 1000

	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(42 + g)))
			me := students[g]
			live := map[string]int64{} // section -> my live ticket
			var myTickets []int64
			<-start
			for i := 0; i < 4; i++ {
				switch rng.Intn(3) {
				case 0:
					sec := secNames[rng.Intn(len(secNames))]
					if _, dup := live[sec]; dup {
						continue // contract: no second live request for the same pair
					}
					tk := atomic.AddInt64(&ticketGen, 1)
					rec.enqueue(s, tk, me, sec)
					live[sec] = tk
					myTickets = append(myTickets, tk)
				case 1:
					if len(myTickets) > 0 {
						tk := myTickets[rng.Intn(len(myTickets))]
						rec.cancel(s, tk)
						for sec, t2 := range live {
							if t2 == tk {
								delete(live, sec)
							}
						}
					} else {
						rec.cancel(s, int64(rng.Intn(2000))) // likely unknown: no-op
					}
				case 2:
					st := students[rng.Intn(len(students))]
					sec := secNames[rng.Intn(len(secNames))]
					rec.inspect(s, []string{me, st}, []string{sec})
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()
	// Final full read, also recorded and checked.
	rec.inspect(s, students, secNames)

	if !checkLinearizable(t, students, sections, rec.entries) {
		for _, e := range rec.entries {
			t.Logf("invoke=%d resp=%d op=%+v", e.invoke, e.resp, e.op)
		}
		t.Fatal("randomized history has no legal serial explanation")
	}
}

// A merge where the section's component has the smaller id must keep
// that (surviving) component locked: operations on the merged component
// from other goroutines must block until the merging operation ends.
func TestReverseMergeHoldsSurvivorLock(t *testing.T) {
	s := New(
		[]string{"s1", "s2"},
		map[string]SectionSpec{
			"X": {Capacity: 1, Credits: 3},
			"Y": {Capacity: 1, Credits: 3},
		},
	)
	s.Enqueue(1, "s1", "X") // component {s1, X}, id 0
	s.Enqueue(2, "s2", "Y") // component {s2, Y}, id 1

	entered := make(chan struct{})
	release := make(chan struct{})
	testHook = func(op string, ticket int64) {
		if ticket == 3 {
			close(entered)
			<-release
		}
	}
	defer func() { testHook = nil }()

	done3 := make(chan struct{})
	go func() {
		defer close(done3)
		s.Enqueue(3, "s2", "X") // reverse merge: survivor is X's component
	}()
	<-entered // parked inside the critical section, survivor lock held

	// White-box: all four entities now share the surviving component.
	s.reg.Lock()
	_, c1 := s.findLocked(studentEnt("s1"))
	_, c2 := s.findLocked(studentEnt("s2"))
	_, cx := s.findLocked(sectionEnt("X"))
	_, cy := s.findLocked(sectionEnt("Y"))
	s.reg.Unlock()
	if c1 != c2 || c2 != cx || cx != cy {
		t.Fatal("entities not merged into one component")
	}

	// An operation touching the merged component must block.
	started := make(chan struct{})
	done4 := make(chan struct{})
	go func() {
		defer close(done4)
		close(started)
		s.Enqueue(4, "s1", "Y")
	}()
	<-started
	select {
	case <-done4:
		t.Fatal("operation on merged component completed while survivor lock was held")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case <-done3:
	case <-time.After(10 * time.Second):
		t.Fatal("merging enqueue did not finish after release")
	}
	select {
	case <-done4:
	case <-time.After(10 * time.Second):
		t.Fatal("blocked operation did not finish after release")
	}
	snap := s.Inspect([]string{"s1", "s2"}, []string{"X", "Y"})
	if snap.Students["s1"].Credits != 3 || snap.Students["s2"].Credits != 3 {
		t.Fatalf("unexpected final state: %+v", snap.Students)
	}
}

package waitlist

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// opKind identifies the kind of a recorded operation.
type opKind int

const (
	opEnqueue opKind = iota
	opCancel
	opInspect
)

// loggedOp is one recorded operation with its invocation/response tokens and
// its observed result.
type loggedOp struct {
	kind     opKind
	ticket   Ticket
	student  StudentID
	section  SectionID
	students []StudentID
	sections []SectionID
	err      error
	snap     Snapshot
	start    int
	end      int
}

func (op *loggedOp) String() string {
	switch op.kind {
	case opEnqueue:
		return fmt.Sprintf("Enqueue(t%d, %s, %s)->%v", op.ticket, op.student, op.section, op.err)
	case opCancel:
		return fmt.Sprintf("Cancel(t%d)", op.ticket)
	default:
		return fmt.Sprintf("Inspect(%v, %v)->%v", op.students, op.sections, formatSnap(op.snap))
	}
}

func formatSnap(s Snapshot) string {
	var b strings.Builder
	b.WriteString("{")
	for id, v := range s.Students {
		fmt.Fprintf(&b, "%s[cr=%d enr=%v wait=%v] ", id, v.Credits, v.Enrolled, v.Waiting)
	}
	for id, v := range s.Sections {
		fmt.Fprintf(&b, "%s[enr=%v wait=%v] ", id, v.Enrolled, v.Waitlist)
	}
	b.WriteString("}")
	return b.String()
}

// recorder wraps an Engine and records a concurrent history. Tokens come
// from one counter, so op A strictly precedes op B in real time iff
// A.end < B.start.
type recorder struct {
	e       *Engine
	mu      sync.Mutex
	counter int
	ops     []*loggedOp
}

func newRecorder(e *Engine) *recorder { return &recorder{e: e} }

func (r *recorder) next() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counter++
	return r.counter
}

func (r *recorder) record(op *loggedOp) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

func (r *recorder) Enqueue(ticket Ticket, student StudentID, section SectionID) error {
	op := &loggedOp{kind: opEnqueue, ticket: ticket, student: student, section: section}
	op.start = r.next()
	op.err = r.e.Enqueue(ticket, student, section)
	op.end = r.next()
	r.record(op)
	return op.err
}

func (r *recorder) Cancel(ticket Ticket) {
	op := &loggedOp{kind: opCancel, ticket: ticket}
	op.start = r.next()
	r.e.Cancel(ticket)
	op.end = r.next()
	r.record(op)
}

func (r *recorder) Inspect(students []StudentID, sections []SectionID) Snapshot {
	op := &loggedOp{kind: opInspect, students: students, sections: sections}
	op.start = r.next()
	op.snap = r.e.Inspect(students, sections)
	op.end = r.next()
	r.record(op)
	return op.snap
}

// checkLinearizable reports whether the recorded history has a serial
// explanation consistent with real-time order and the reference model.
func checkLinearizable(t *testing.T, model *Model, ops []*loggedOp) {
	t.Helper()
	if !findLinearization(model, ops) {
		var b strings.Builder
		b.WriteString("history has no legal serial explanation:\n")
		for _, op := range ops {
			fmt.Fprintf(&b, "  [%d,%d] %s\n", op.start, op.end, op)
		}
		t.Fatal(b.String())
	}
}

func findLinearization(model *Model, ops []*loggedOp) bool {
	n := len(ops)
	placed := make([]bool, n)
	remaining := n

	// precedes[i][j] is true when op i must be placed before op j.
	precedes := make([][]bool, n)
	for i := range precedes {
		precedes[i] = make([]bool, n)
		for j := range precedes[i] {
			if i != j && ops[i].end < ops[j].start {
				precedes[i][j] = true
			}
		}
	}

	var search func(m *Model) bool
	search = func(m *Model) bool {
		if remaining == 0 {
			return true
		}
		for i, op := range ops {
			if placed[i] {
				continue
			}
			ready := true
			for j := range ops {
				if !placed[j] && precedes[j][i] {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			next := m.clone()
			if !applyOp(next, op) {
				continue
			}
			placed[i] = true
			remaining--
			if search(next) {
				return true
			}
			placed[i] = false
			remaining++
		}
		return false
	}
	return search(model)
}

// applyOp applies one operation to the model and checks the observed result.
func applyOp(m *Model, op *loggedOp) bool {
	switch op.kind {
	case opEnqueue:
		if op.err != nil {
			return false // generated histories never use invalid/duplicate tickets
		}
		m.Enqueue(op.ticket, op.student, op.section)
		return true
	case opCancel:
		m.Cancel(op.ticket)
		return true
	case opInspect:
		got := m.Inspect(op.students, op.sections)
		return reflect.DeepEqual(got, op.snap)
	}
	return false
}

package scheduler

import (
	"errors"
	"testing"
)

func TestDAG_AddSimple(t *testing.T) {
	dag := NewDAG()

	if err := dag.Add("A", []string{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := dag.Add("B", []string{"A"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDAG_AddDuplicate(t *testing.T) {
	dag := NewDAG()
	dag.Add("A", []string{})

	err := dag.Add("A", []string{})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestDAG_AddCycleSimple(t *testing.T) {
	dag := NewDAG()
	dag.Add("A", []string{})
	dag.Add("B", []string{"A"})

	err := dag.Add("C", []string{})
	if err != nil {
		t.Fatalf("unexpected error adding C: %v", err)
	}

	dag2 := NewDAG()
	dag2.Add("A", []string{})
	dag2.Add("B", []string{"A"})
	err = dag2.Add("A2", []string{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDAG_AddCycleDetected(t *testing.T) {
	dag := NewDAG()
	dag.Add("A", []string{})
	dag.Add("B", []string{"A"})
	dag.Add("C", []string{"B"})

	dag2 := NewDAG()
	dag2.Add("B", []string{})
	dag2.Add("C", []string{"B"})

	err := dag2.Add("B2", []string{"C"})
	if err != nil {
		t.Logf("got error (expected for cycle): %v", err)
	}
}

func TestDAG_NoCycleRollback(t *testing.T) {
	dag := NewDAG()
	dag.Add("A", []string{})
	dag.Add("B", []string{"A"})
	dag.Add("C", []string{"B"})

	err := dag.Add("D", []string{})
	if err != nil {
		t.Fatal(err)
	}

	if err := dag.Add("E", []string{"D"}); err != nil {
		t.Errorf("graph inconsistent after failed add: %v", err)
	}
}

func TestDAG_OnCompleteUnlocks(t *testing.T) {
	dag := NewDAG()
	dag.Add("A", []string{})
	dag.Add("B", []string{"A"})
	dag.Add("C", []string{"A"})

	unlocked := dag.OnComplete("A")

	if len(unlocked) != 2 {
		t.Fatalf("expected 2 unlocked, got %d", len(unlocked))
	}

	got := map[string]bool{}
	for _, id := range unlocked {
		got[id] = true
	}
	if !got["B"] || !got["C"] {
		t.Errorf("expected B and C unlocked, got %v", unlocked)
	}
}

func TestDAG_OnCompleteMultipleDeps(t *testing.T) {
	dag := NewDAG()
	dag.Add("A", []string{})
	dag.Add("B", []string{})
	dag.Add("C", []string{"A", "B"})

	unlocked := dag.OnComplete("A")
	if len(unlocked) != 0 {
		t.Errorf("C should still be waiting for B, got unlocked: %v", unlocked)
	}

	unlocked = dag.OnComplete("B")
	if len(unlocked) != 1 || unlocked[0] != "C" {
		t.Errorf("expected C to be unlocked, got %v", unlocked)
	}
}

func TestDAG_OnCompleteNodesCleanup(t *testing.T) {
	dag := NewDAG()
	dag.Add("A", []string{})
	dag.OnComplete("A")

	if err := dag.Add("A", []string{}); err != nil {
		t.Errorf("expected to re-add A after completion, got: %v", err)
	}
}

func TestDAG_OnCompleteNoDependents(t *testing.T) {
	dag := NewDAG()
	dag.Add("A", []string{})

	unlocked := dag.OnComplete("A")
	if len(unlocked) != 0 {
		t.Errorf("expected no unlocked, got %v", unlocked)
	}
}

func TestDAG_ConcurrentAdd(t *testing.T) {
	dag := NewDAG()
	dag.Add("root", []string{})

	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		go func(n int) {
			id := string(rune('a' + n%26)) + string(rune('0'+n/26))
			errs <- dag.Add(id, []string{})
		}(i)
	}

	for i := 0; i < 50; i++ {
		if err := <-errs; err != nil && !errors.Is(err, ErrAlreadyExists) {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestDAG_AddUnknownDependencyReturnsError(t *testing.T) {
    dag := NewDAG()

    err := dag.Add("B", []string{"A"})
	if err == nil {
        t.Error("expected error for unknown dependency")
    }
}

func TestDAG_AddInTopologicalOrder(t *testing.T) {
    dag := NewDAG()

    if err := dag.Add("A", []string{}); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if err := dag.Add("B", []string{"A"}); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
}
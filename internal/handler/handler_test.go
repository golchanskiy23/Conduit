package handler

import (
	"bytes"
	"conduit/internal/ds/graph"
	"conduit/internal/ds/queue/heap"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"
)

var sha256Regex = regexp.MustCompile(`^[0-9a-f]{64}$`)

type mockSubmitter struct {
	lastJob  *heap.Item
	lastDeps []string
	err      error
}

func (m *mockSubmitter) Submit(job *heap.Item, deps []string) error {
	m.lastJob = job
	m.lastDeps = deps
	return m.err
}

func makeRequest(t *testing.T, body any) *http.Request {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func newTestHandler(sub Submitter) *JobHandler {
    counter := 0
    return &JobHandler{
        scheduler: sub,
        generateID: func() (string, error) {
            counter++
            return fmt.Sprintf("%064x", counter), nil
        },
    }
}

func TestEnqueueJob_Success(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	runAt := time.Now().Add(time.Minute)
	body := EnqueueRequest{
		Priority: heap.PriorityHigh,
		RunAt:    runAt,
		Deps:     []string{"dep1"},
	}

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, body))

	if rr.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d", rr.Code)
	}
	if sub.lastJob.Priority != heap.PriorityHigh {
		t.Errorf("expected PriorityHigh, got %v", sub.lastJob.Priority)
	}
	if !sub.lastJob.RunAt.Equal(runAt) {
		t.Errorf("RunAt mismatch")
	}
	if len(sub.lastDeps) != 1 || sub.lastDeps[0] != "dep1" {
		t.Errorf("expected deps [dep1], got %v", sub.lastDeps)
	}
}

func TestEnqueueJob_GeneratesJobID(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{Priority: heap.PriorityNormal}))

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr.Code)
	}
	if sub.lastJob.JobID == "" {
		t.Error("expected non-empty JobID")
	}
}

func TestEnqueueJob_JobIDIsSHA256Hex(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{}))

	if !sha256Regex.MatchString(sub.lastJob.JobID) {
		t.Errorf("JobID is not valid sha256 hex: %q", sub.lastJob.JobID)
	}
}

func TestEnqueueJob_UniqueJobIDs(t *testing.T) {
	ids := map[string]bool{}
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	for i := 0; i < 100; i++ {
		rr := httptest.NewRecorder()
		h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{}))
		if rr.Code != http.StatusAccepted {
			t.Fatalf("request %d: expected 202, got %d", i, rr.Code)
		}
		id := sub.lastJob.JobID
		if ids[id] {
			t.Fatalf("collision detected on iteration %d: %s", i, id)
		}
		ids[id] = true
	}
}

func TestEnqueueJob_ResponseContainsJobID(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{}))

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["job_id"] == "" {
		t.Error("expected job_id in response body")
	}
	if resp["job_id"] != sub.lastJob.JobID {
		t.Errorf("response job_id %s != submitted job_id %s", resp["job_id"], sub.lastJob.JobID)
	}
}

func TestEnqueueJob_ResponseJobIDIsSHA256Hex(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{}))

	var resp map[string]string
	json.NewDecoder(rr.Body).Decode(&resp)

	if !sha256Regex.MatchString(resp["job_id"]) {
		t.Errorf("response job_id is not valid sha256 hex: %q", resp["job_id"])
	}
}

func TestEnqueueJob_CollisionReturns409(t *testing.T) {
	sub := &mockSubmitter{err: graph.ErrAlreadyExists}
	h := NewHTTPHandler(sub, 10*time.Minute)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{}))

	if rr.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d", rr.Code)
	}
}

func TestEnqueueJob_InvalidJSON(t *testing.T) {
	h := newTestHandler(&mockSubmitter{})

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader([]byte("not json")))
	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestEnqueueJob_SubmitErrorReturns500(t *testing.T) {
	sub := &mockSubmitter{err: errors.New("internal error")}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{}))

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

func TestEnqueueJob_NoWriteAfterError(t *testing.T) {
	sub := &mockSubmitter{err: errors.New("fail")}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{}))

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected only 500, got %d", rr.Code)
	}
}

func TestEnqueueJob_NoDeps(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{Priority: heap.PriorityLow}))

	if rr.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d", rr.Code)
	}
	if len(sub.lastDeps) != 0 {
		t.Errorf("expected empty deps, got %v", sub.lastDeps)
	}
}

func TestEnqueueJob_ContentTypeJSON(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{}))

	ct := rr.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}
}
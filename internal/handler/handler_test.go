package handler

import (
	"bytes"
	"conduit/internal/ds/graph"
	"conduit/internal/ds/queue/heap"
	"conduit/internal/ds/ttlmap"
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

type countingSubmitter struct {
	inner *mockSubmitter
	calls int
}

func (c *countingSubmitter) Submit(job *heap.Item, deps []string) error {
	c.calls++
	return c.inner.Submit(job, deps)
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
		ttlMap: ttlmap.New(10 * time.Minute),
	}
}

func TestEnqueueJob_Success(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	runAt := time.Now().Add(time.Minute)
	body := EnqueueRequest{
		JobType:  "payment.charge",
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

func TestEnqueueJob_JobTypePropagated(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "email.send"}))

	if sub.lastJob.JobType != "email.send" {
		t.Errorf("expected email.send, got %s", sub.lastJob.JobType)
	}
}

func TestEnqueueJob_GeneratesJobID(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "email.send"}))

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
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "report.generate"}))

	if !sha256Regex.MatchString(sub.lastJob.JobID) {
		t.Errorf("JobID is not valid sha256 hex: %q", sub.lastJob.JobID)
	}
}

func TestEnqueueJob_UniqueJobIDs(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)
	ids := map[string]bool{}

	for i := 0; i < 100; i++ {
		rr := httptest.NewRecorder()
		h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{
			JobType: fmt.Sprintf("job.type.%d", i),
		}))
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
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "payment.charge"}))

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
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "payment.charge"}))

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
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "payment.unique"}))

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
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "payment.error"}))

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

func TestEnqueueJob_NoWriteAfterError(t *testing.T) {
	sub := &mockSubmitter{err: errors.New("fail")}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "payment.fail"}))

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected only 500, got %d", rr.Code)
	}
}

func TestEnqueueJob_NoDeps(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr := httptest.NewRecorder()
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{
		JobType:  "email.send",
		Priority: heap.PriorityLow,
	}))

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
	h.EnqueueJob(rr, makeRequest(t, EnqueueRequest{JobType: "email.send"}))

	ct := rr.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}
}

func TestEnqueueJob_IdempotentRequest(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	body := EnqueueRequest{
		JobType:  "payment.charge",
		Priority: heap.PriorityNormal,
		RunAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	rr1 := httptest.NewRecorder()
	h.EnqueueJob(rr1, makeRequest(t, body))
	var resp1 map[string]string
	json.NewDecoder(rr1.Body).Decode(&resp1)
	firstID := resp1["job_id"]

	rr2 := httptest.NewRecorder()
	h.EnqueueJob(rr2, makeRequest(t, body))
	var resp2 map[string]string
	json.NewDecoder(rr2.Body).Decode(&resp2)

	if rr2.Code != http.StatusAccepted {
		t.Errorf("expected 202 on duplicate, got %d", rr2.Code)
	}
	if resp2["job_id"] != firstID {
		t.Errorf("expected same job_id %s, got %s", firstID, resp2["job_id"])
	}
}

func TestEnqueueJob_DifferentJobTypeDifferentJob(t *testing.T) {
	sub := &mockSubmitter{}
	h := newTestHandler(sub)

	rr1 := httptest.NewRecorder()
	h.EnqueueJob(rr1, makeRequest(t, EnqueueRequest{JobType: "payment.charge"}))
	var resp1 map[string]string
	json.NewDecoder(rr1.Body).Decode(&resp1)

	rr2 := httptest.NewRecorder()
	h.EnqueueJob(rr2, makeRequest(t, EnqueueRequest{JobType: "email.send"}))
	var resp2 map[string]string
	json.NewDecoder(rr2.Body).Decode(&resp2)

	if resp1["job_id"] == resp2["job_id"] {
		t.Error("expected different job_ids for different job types")
	}
}

func TestEnqueueJob_TTLMapClearedOnSubmitError(t *testing.T) {
	sub := &mockSubmitter{err: errors.New("fail")}
	h := newTestHandler(sub)

	body := EnqueueRequest{JobType: "payment.rollback"}

	rr1 := httptest.NewRecorder()
	h.EnqueueJob(rr1, makeRequest(t, body))
	if rr1.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr1.Code)
	}

	sub.err = nil
	rr2 := httptest.NewRecorder()
	h.EnqueueJob(rr2, makeRequest(t, body))
	if rr2.Code != http.StatusAccepted {
		t.Errorf("expected 202 after error cleared, got %d", rr2.Code)
	}
}

func TestEnqueueJob_IdempotentDoesNotCallSubmitTwice(t *testing.T) {
	sub := &mockSubmitter{}
	counting := &countingSubmitter{inner: sub}
	counter := 0
	h := &JobHandler{
		scheduler: counting,
		generateID: func() (string, error) {
			counter++
			return fmt.Sprintf("%064x", counter), nil
		},
		ttlMap: ttlmap.New(10 * time.Minute),
	}

	body := EnqueueRequest{JobType: "payment.count"}
	h.EnqueueJob(httptest.NewRecorder(), makeRequest(t, body))
	h.EnqueueJob(httptest.NewRecorder(), makeRequest(t, body))

	if counting.calls != 1 {
		t.Errorf("expected Submit called once, got %d", counting.calls)
	}
}

func TestEnqueueJob_TTLExpiredAllowsNewJob(t *testing.T) {
	sub := &mockSubmitter{}
	h := &JobHandler{
		scheduler:  sub,
		generateID: generateJobID,
		ttlMap:     ttlmap.New(50 * time.Millisecond),
	}

	body := EnqueueRequest{JobType: "payment.ttl"}

	rr1 := httptest.NewRecorder()
	h.EnqueueJob(rr1, makeRequest(t, body))
	var resp1 map[string]string
	json.NewDecoder(rr1.Body).Decode(&resp1)

	time.Sleep(100 * time.Millisecond)

	rr2 := httptest.NewRecorder()
	h.EnqueueJob(rr2, makeRequest(t, body))
	var resp2 map[string]string
	json.NewDecoder(rr2.Body).Decode(&resp2)

	if resp1["job_id"] == resp2["job_id"] {
		t.Error("expected new job_id after TTL expiry")
	}
}
package handler

import (
	"conduit/internal/ds/graph"
	"conduit/internal/ds/queue/heap"
	ttl "conduit/internal/ds/ttlmap"
	"conduit/pkg/ratelimit"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Submitter interface {
	Submit(job *heap.Item, deps []string) error
}

type IDGenerator func() (string, error)

type JobHandler struct {
	scheduler   Submitter
	generateID  IDGenerator
	ttlMap      *ttl.TTLMap
	rateLimiter *ratelimit.SlidingWindow
}

type HandlerOption func(*JobHandler)

func WithRateLimiter(rl *ratelimit.SlidingWindow) HandlerOption {
	return func(h *JobHandler) {
		h.rateLimiter = rl
	}
}

func NewHTTPHandler(s Submitter, t time.Duration, opts ...HandlerOption) *JobHandler {
	h := &JobHandler{
		scheduler:  s,
		generateID: generateJobID,
		ttlMap:     ttl.New(t),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

type EnqueueRequest struct {
	Priority heap.Priority   `json:"priority"`
	JobType  string          `json:"job_type"`
	RunAt    time.Time       `json:"run_at"`
	Deps     []string        `json:"deps"`
	Payload  json.RawMessage `json:"payload"`
}

func generateJobID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(id.String()))
	return fmt.Sprintf("%x", hash), nil
}

func makeTTLKey(req *EnqueueRequest) string {
	key := fmt.Sprintf("%s|%d|%s|%s",
		req.JobType,
		req.Priority,
		req.RunAt.UTC().Format(time.RFC3339Nano),
		strings.Join(req.Deps, ","),
	)
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", sum)
}

func (h *JobHandler) EnqueueJob(w http.ResponseWriter, r *http.Request) {
	if h.rateLimiter != nil && !h.rateLimiter.Allow() {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	jobID, err := h.generateID()
	if err != nil {
		http.Error(w, "failed to generate job id", http.StatusInternalServerError)
		return
	}

	job := &heap.Item{
		JobID:      jobID,
		JobType:    req.JobType,
		Payload:    req.Payload,
		Priority:   req.Priority,
		RunAt:      req.RunAt,
		EnqueuedAt: time.Now(),
	}

	key := makeTTLKey(&req)
	if existing, created := h.ttlMap.SetIfAbsent(key, job); !created {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"job_id": existing.JobID})
		return
	}

	if err := h.scheduler.Submit(job, req.Deps); err != nil {
		if errors.Is(err, graph.ErrAlreadyExists) {
			http.Error(w, "job already exists", http.StatusConflict)
			return
		}
		http.Error(w, "failed to submit job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID})
}
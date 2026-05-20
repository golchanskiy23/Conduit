package handler

import (
	"conduit/internal/ds/graph"
	"conduit/internal/ds/queue/heap"
	ttl "conduit/internal/ds/ttlmap"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type Submitter interface {
	Submit(job *heap.Item, deps []string) error
}

type IDGenerator func() (string, error)

type JobHandler struct {
	scheduler Submitter
	generateID  IDGenerator
	ttlMap *ttl.TTLMap
}

func NewHTTPHandler(s Submitter, t time.Duration) *JobHandler {
	return &JobHandler{
		scheduler: s,
		generateID: generateJobID,
		ttlMap: ttl.New(t),
	}
}

type EnqueueRequest struct {
	Priority heap.Priority `json:"priority"`
	RunAt    time.Time         `json:"run_at"`
	Deps     []string          `json:"deps"`
}

func generateJobID() (string, error) {
    id, err := uuid.NewRandom()
    if err != nil {
        return "", err
    }
    hash := sha256.Sum256([]byte(id.String()))
    return fmt.Sprintf("%x", hash), nil
}

func (h *JobHandler) EnqueueJob(w http.ResponseWriter, r *http.Request) {
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
		JobID:    jobID,
		Priority: req.Priority,
		RunAt: req.RunAt,
		EnqueuedAt: time.Now(),
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
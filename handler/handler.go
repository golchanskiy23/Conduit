package handler

import (
	"conduit/internal/scheduler"
	"encoding/json"
	"net/http"
	"time"
	"crypto/sha256"
    "fmt"
    "github.com/google/uuid"
	"errors"
)

type Submitter interface {
	Submit(job *scheduler.Item, deps []string) error
}

type JobHandler struct {
	scheduler Submitter
}

func NewHTTPHandler(s Submitter) *JobHandler {
	return &JobHandler{scheduler: s}
}

type EnqueueRequest struct {
	Priority scheduler.Priority `json:"priority"`
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

	jobID, err := generateJobID()
    if err != nil {
        http.Error(w, "failed to generate job id", http.StatusInternalServerError)
        return
    }

	job := &scheduler.Item{
		JobID:    jobID,
		Priority: req.Priority,
		RunAt: req.RunAt,
		EnqueuedAt: time.Now(),
	}

	if err := h.scheduler.Submit(job, req.Deps); err != nil {
		if errors.Is(err, scheduler.ErrAlreadyExists) {
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
package handler

import (
	"conduit/internal/scheduler"
	"encoding/json"
	"net/http"
	"time"
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
	JobID    string            `json:"job_id"`
	Priority scheduler.Priority `json:"priority"`
	RunAt    time.Time         `json:"run_at"`
	Deps     []string          `json:"deps"`
}

func (h *JobHandler) EnqueueJob(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	job := &scheduler.Item{
		JobID:    req.JobID,
		Priority: req.Priority,
		RunAt: req.RunAt,
		EnqueuedAt: time.Now(),
	}

	if err := h.scheduler.Submit(job, req.Deps); err != nil {
		http.Error(w, "failed to submit job", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
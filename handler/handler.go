package handler

import (
	"conduit/internal/sheduler"
	"encoding/json"
	"net/http"
	"time"
)

type JobHandler struct{
	scheduler *sheduler.Scheduler
}

type EnqueueResponse struct{
	JobID      string `json:"job_id"`
	Priority   int `json:"priority"`
	RunAt time.Time `json:"run_at"`
	Indegree []string `json:"indegree"`
}

// через scheduler вызываем функцию добавления Item в граф и детекцию цикла
func (handler *JobHandler) EnqueueJob(writer http.ResponseWriter,r *http.Response){
	var response EnqueueResponse
	if err := json.NewDecoder(r.Body).Decode(&response); err != nil{
		http.Error(writer, "error in decoding input", http.StatusBadRequest)
		return
	}

	// формируем job
	job := &sheduler.Item{
		JobID: response.JobID,
		Priority: response.Priority,
		EnqueuedAt: time.Now(),
	}

	// обработка ошибок - подумать над кастомными типами и вариациями
	if err := handler.scheduler.Submit(job, response.Indegree, response.RunAt); err != nil{
		http.Error(writer, "error during submitting item", http.StatusInternalServerError)
	}

	writer.WriteHeader(http.StatusAccepted)
}
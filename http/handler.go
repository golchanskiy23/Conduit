package http

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
	EnqueuedAt time.Time `json:"enqueued_at"`
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
	}

	// обработка ошибок - подумать над кастомными типами и вариациями
	if err := handler.scheduler.Submit(job); err != nil{
		http.Error(writer, "error during submitting item", http.StatusInternalServerError)
	}

	writer.WriteHeader(http.StatusAccepted)
}
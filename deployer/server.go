package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/flynn/flynn/deployer/types"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/postgres"
	"github.com/flynn/flynn/pkg/random"
	"github.com/julienschmidt/httprouter"
)

var db *postgres.DB

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "4000"
	}

	addr := ":" + port
	if err := discoverd.Register("flynn-deployer", addr); err != nil {
		log.Fatal(err)
	}

	var err error
	db, err = postgres.Open("", "")
	if err != nil {
		log.Fatal(err)
	}

	if err := migrateDB(db.DB); err != nil {
		log.Fatal(err)
	}

	router := httprouter.New()
	router.POST("/jobs", queueJob)
	router.GET("/jobs/:jobs_id", getJob)

	log.Fatal(http.ListenAndServe(addr, router))
}

func queueJob(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
	job := &deployer.Job{}
	if err := json.NewDecoder(req.Body).Decode(job); err != nil {
		http.Error(w, err.Error(), 400)
	}
	steps, err := json.Marshal(job.Steps)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if job.ID == "" {
		job.ID = random.UUID()
	}
	if err := db.QueryRow("INSERT INTO jobs (job_id, steps) VALUES ($1, $2) RETURNING created_at", job.ID, steps).Scan(&job.CreatedAt); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	job.ID = postgres.CleanUUID(job.ID)
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(job); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func getJob(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
	var steps []byte
	job := &deployer.Job{}
	err := db.QueryRow("SELECT job_id, steps, created_at FROM jobs WHERE job_id = $1", params.ByName("jobs_id")).Scan(&job.ID, &steps, &job.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			w.WriteHeader(404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}
	job.ID = postgres.CleanUUID(job.ID)
	if err := json.Unmarshal(steps, &job.Steps); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(job); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

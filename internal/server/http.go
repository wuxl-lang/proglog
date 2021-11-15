package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHttpServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()

	// register handle
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")

	// Set up HTTP Sever
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

// A server holds Log
type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	// Unmarshal request
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	// Append record
	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Marshal response
	var res = ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	// Unmarshal request
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	// Read record in Log
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound { // It should be bad request
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}
	if err != nil { // Otherwise,
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	// Marshal response
	var res = ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
}

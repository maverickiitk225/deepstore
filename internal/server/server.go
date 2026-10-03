package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/deepanker/deepstore/internal/engine"
)

const maxBody = 16<<20 + 1024

type Server struct {
	eng  *engine.Engine
	http *http.Server
}

func New(eng *engine.Engine, addr string) *Server {
	s := &Server{eng: eng}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/keys/{key}", s.put)
	mux.HandleFunc("GET /v1/keys/{key}", s.get)
	mux.HandleFunc("DELETE /v1/keys/{key}", s.delete)
	mux.HandleFunc("POST /v1/keys/{key}/cas", s.cas)
	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) Handler() http.Handler {
	return s.http.Handler
}

func (s *Server) ListenAndServe() error {
	return s.http.ListenAndServe()
}

func (s *Server) Serve(l net.Listener) error {
	return s.http.Serve(l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var body struct {
		Value string `json:"value"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	index, err := s.eng.Put(key, body.Value)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Index uint64 `json:"index"`
	}{Index: index})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	value, ok := s.eng.Get(r.PathValue("key"))
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("not found"))
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Value string `json:"value"`
	}{Value: value})
}

func (s *Server) cas(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var body struct {
		Expected string `json:"expected"`
		Value    string `json:"value"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	index, swapped, err := s.eng.CAS(key, body.Expected, body.Value)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Index   uint64 `json:"index"`
		Swapped bool   `json:"swapped"`
	}{Index: index, Swapped: swapped})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	index, err := s.eng.Delete(r.PathValue("key"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Index uint64 `json:"index"`
	}{Index: index})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: err.Error()})
}

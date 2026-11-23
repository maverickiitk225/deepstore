package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/deepanker/deepstore/internal/errs"
)

const maxBody = 16<<20 + 1024

type storage interface {
	Put(key, value string, clientID, seq uint64) (uint64, error)
	Delete(key string, clientID, seq uint64) (uint64, error)
	CAS(key, expected, value string, clientID, seq uint64) (uint64, bool, error)
	Get(key string) (string, bool, error)
}

type Server struct {
	store storage
	http  *http.Server
}

func New(store storage, addr string) *Server {
	s := &Server{store: store}
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
		Value    string `json:"value"`
		ClientID uint64 `json:"client_id"`
		Seq      uint64 `json:"seq"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := requireSession(body.ClientID, body.Seq); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	index, err := s.store.Put(key, body.Value, body.ClientID, body.Seq)
	if err != nil {
		writeStatus(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Index uint64 `json:"index"`
	}{Index: index})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	value, ok, err := s.store.Get(r.PathValue("key"))
	if err != nil {
		writeStatus(w, err)
		return
	}
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
		ClientID uint64 `json:"client_id"`
		Seq      uint64 `json:"seq"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := requireSession(body.ClientID, body.Seq); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	index, swapped, err := s.store.CAS(key, body.Expected, body.Value, body.ClientID, body.Seq)
	if err != nil {
		writeStatus(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Index   uint64 `json:"index"`
		Swapped bool   `json:"swapped"`
	}{Index: index, Swapped: swapped})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ClientID uint64 `json:"client_id"`
		Seq      uint64 `json:"seq"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := requireSession(body.ClientID, body.Seq); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	index, err := s.store.Delete(r.PathValue("key"), body.ClientID, body.Seq)
	if err != nil {
		writeStatus(w, err)
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

func requireSession(clientID, seq uint64) error {
	if clientID == 0 || seq == 0 {
		return errors.New("client_id and seq are required")
	}
	return nil
}

func writeStatus(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errs.ErrSession):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, errs.ErrNotLeader):
		writeError(w, http.StatusServiceUnavailable, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: err.Error()})
}

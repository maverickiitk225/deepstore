package server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/deepanker/deepstore/internal/node"
	"github.com/prometheus/client_golang/prometheus"
)

type reporter interface {
	Status() (node.Status, error)
}

type healthBody struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthBody{OK: true})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	rep, ok := s.store.(reporter)
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("status is not supported"))
		return
	}
	st, err := rep.Status()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, healthBody{Reason: err.Error()})
		return
	}
	ready, reason := node.Ready(st, time.Now())
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, healthBody{Reason: reason})
		return
	}
	writeJSON(w, http.StatusOK, healthBody{OK: true})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	rep, ok := s.store.(reporter)
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("status is not supported"))
		return
	}
	st, err := rep.Status()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

var (
	httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "deepstore_http_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "code"})
	httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "deepstore_http_requests_total",
		Help: "HTTP requests.",
	}, []string{"method", "route", "code"})
)

func init() {
	prometheus.MustRegister(httpDuration, httpRequests)
}

func instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			code := rec.code
			if code == 0 {
				code = http.StatusOK
			}
			route := routeOf(r)
			codeText := strconv.Itoa(code)
			httpDuration.WithLabelValues(r.Method, route, codeText).Observe(time.Since(start).Seconds())
			httpRequests.WithLabelValues(r.Method, route, codeText).Inc()
		}()
		next.ServeHTTP(rec, r)
	})
}

func routeOf(r *http.Request) string {
	pattern := r.Pattern
	if pattern == "" {
		return r.URL.Path
	}
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		return pattern[i+1:]
	}
	return pattern
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

// RegisterNodeMetrics must be called once. A scrape reads n then.
func RegisterNodeMetrics(n interface{ Status() (node.Status, error) }) {
	prometheus.MustRegister(newNodeCollector(n))
}

type nodeCollector struct {
	status   func() (node.Status, error)
	commit   *prometheus.Desc
	applied  *prometheus.Desc
	lag      *prometheus.Desc
	term     *prometheus.Desc
	leader   *prometheus.Desc
	keys     *prometheus.Desc
	sessions *prometheus.Desc
}

func newNodeCollector(n interface{ Status() (node.Status, error) }) *nodeCollector {
	return &nodeCollector{
		status:   n.Status,
		commit:   prometheus.NewDesc("deepstore_raft_commit_index", "Raft commit index of this node.", nil, nil),
		applied:  prometheus.NewDesc("deepstore_raft_applied_index", "Index applied to the store on this node.", nil, nil),
		lag:      prometheus.NewDesc("deepstore_raft_apply_lag", "Commit index minus the applied index on this node.", nil, nil),
		term:     prometheus.NewDesc("deepstore_raft_term", "Current Raft term of this node.", nil, nil),
		leader:   prometheus.NewDesc("deepstore_raft_leader", "1 when this node is the leader.", nil, nil),
		keys:     prometheus.NewDesc("deepstore_keys", "Keys in the store.", nil, nil),
		sessions: prometheus.NewDesc("deepstore_sessions", "Client sessions in the store.", nil, nil),
	}
}

func (c *nodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.commit
	ch <- c.applied
	ch <- c.lag
	ch <- c.term
	ch <- c.leader
	ch <- c.keys
	ch <- c.sessions
}

func (c *nodeCollector) Collect(ch chan<- prometheus.Metric) {
	st, err := c.status()
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.commit, err)
		return
	}
	lag := float64(0)
	if st.CommitIndex > st.AppliedIndex {
		lag = float64(st.CommitIndex - st.AppliedIndex)
	}
	isLeader := float64(0)
	if st.State == "Leader" {
		isLeader = 1
	}
	ch <- prometheus.MustNewConstMetric(c.commit, prometheus.GaugeValue, float64(st.CommitIndex))
	ch <- prometheus.MustNewConstMetric(c.applied, prometheus.GaugeValue, float64(st.AppliedIndex))
	ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, lag)
	ch <- prometheus.MustNewConstMetric(c.term, prometheus.GaugeValue, float64(st.Term))
	ch <- prometheus.MustNewConstMetric(c.leader, prometheus.GaugeValue, isLeader)
	ch <- prometheus.MustNewConstMetric(c.keys, prometheus.GaugeValue, float64(st.Keys))
	ch <- prometheus.MustNewConstMetric(c.sessions, prometheus.GaugeValue, float64(st.Sessions))
}

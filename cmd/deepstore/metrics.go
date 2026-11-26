package main

import (
	"sync"

	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/go-metrics/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
)

var (
	metricsOnce sync.Once
	metricsErr  error
)

// enableMetrics must run before the node opens, so the first Raft samples are kept.
func enableMetrics() error {
	metricsOnce.Do(func() {
		metricsErr = startMetrics()
	})
	return metricsErr
}

func startMetrics() error {
	sink, err := prometheus.NewPrometheusSinkFrom(prometheus.PrometheusOpts{
		Expiration: 0,
		Registerer: prom.DefaultRegisterer,
	})
	if err != nil {
		return err
	}
	cfg := metrics.DefaultConfig("deepstore")
	cfg.EnableHostname = false
	cfg.EnableHostnameLabel = true
	cfg.EnableServiceLabel = true
	cfg.EnableRuntimeMetrics = false
	_, err = metrics.NewGlobal(cfg, sink)
	return err
}

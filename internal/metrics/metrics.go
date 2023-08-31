package metrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"

	"github.com/modfin/creek"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var pgInserts = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "creek_consumer_pg_inserts",
	Help: "total numbers of row into postgres from creek streams",
}, []string{"creek_stream_type", "creek_pg_target", "creek_pg_source"})

func init() {
	prometheus.MustRegister(pgInserts)
}

func Start(ctx context.Context, port int) {
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port)}
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		<-ctx.Done()
		cc, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		logrus.Info("shutting down metrics server")
		srv.Shutdown(cc)
		logrus.Info("metrics server is shut down")
	}()

	logrus.Infof("starting metrics server on :%d", port)

	err := srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logrus.Error(err)
	}
}

func AddInserts(streamType creek.StreamType, source string, target string, count int) {
	pgInserts.
		With(map[string]string{"creek_stream_type": string(streamType), "creek_pg_source": source, "creek_pg_target": target}).
		Add(float64(count))
}

func IncInserts(streamType creek.StreamType, source string, target string) {
	pgInserts.
		With(map[string]string{"creek_stream_type": string(streamType), "creek_pg_source": source, "creek_pg_target": target}).
		Inc()
}

// Inspired by https://github.com/weaveworks/promrus/blob/master/promrus.go

type PrometheusHook struct {
	counterVec *prometheus.CounterVec
}

var supportedLevels = []logrus.Level{logrus.DebugLevel, logrus.InfoLevel, logrus.WarnLevel, logrus.ErrorLevel}

func NewPrometheusHook() (*PrometheusHook, error) {
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "log_messages_total",
		Help: "Total number of log messages.",
	}, []string{"level"})
	// Initialise counters for all supported levels:
	for _, level := range supportedLevels {
		counterVec.WithLabelValues(level.String())
	}
	err := prometheus.Register(counterVec)
	if err != nil {
		return nil, err
	}
	return &PrometheusHook{
		counterVec: counterVec,
	}, nil
}

func MustNewPrometheusHook() *PrometheusHook {
	hook, err := NewPrometheusHook()
	if err != nil {
		panic(err)
	}
	return hook
}

func (hook *PrometheusHook) Fire(entry *logrus.Entry) error {
	hook.counterVec.WithLabelValues(entry.Level.String()).Inc()
	return nil
}

func (hook *PrometheusHook) Levels() []logrus.Level {
	return supportedLevels
}

package utils

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"time"
)

// Inspired by https://github.com/misho-kr/logrus-hooks/blob/master/limit.go

type RateLimitedWriter struct {
	limiter   *rate.Limiter
	logger    *logrus.Logger
	discarded int
}

func NewRateLimitedWriter(limitPerSecond float32, burst int, level logrus.Level) *RateLimitedWriter {
	l := logrus.New()
	l.SetLevel(level)
	return &RateLimitedWriter{
		limiter: rate.NewLimiter(rate.Limit(limitPerSecond), burst),
		logger:  l,
	}
}

func (hook *RateLimitedWriter) Fire(entry *logrus.Entry) error {
	if !hook.limiter.Allow() {
		hook.discarded++
		return nil
	}

	hook.logger.WithField("discarded", hook.discarded).Log(entry.Level, entry.Message)
	hook.discarded = 0

	//return errors.New("wow")
	return nil
}

func (hook *RateLimitedWriter) Levels() []logrus.Level {
	return logrus.AllLevels
}

func DefaultBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.MaxInterval = 15 * time.Second
	b.MaxElapsedTime = 1<<63 - 1
	return b
}

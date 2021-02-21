package namespace

import (
	"github.com/go-logr/logr"
)

// nolint:gochecknoglobals
var (
	// DefaultLogger is used by the Namespace Service by default. It discards
	// all the logs.
	DefaultLogger = logr.Discard()
)

type Option func(svc *service)

// WithLogger configures the Service to use the logr-compatible Logger. Any
// compatible instance can be passed.
func WithLogger(log logr.Logger) Option {
	return func(svc *service) {
		svc.log = log
	}
}

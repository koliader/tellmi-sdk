package health

import (
	"context"
	"net/http"
	"time"
)

// Check reports whether a dependency is healthy.
type Check func(ctx context.Context) error

type Server struct {
	addr    string
	checks  []Check
	timeout time.Duration
}

func NewServer(addr string, checks ...Check) *Server {
	return &Server{
		addr:    addr,
		checks:  checks,
		timeout: 2 * time.Second,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return srv.ListenAndServe()
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	for _, check := range s.checks {
		if err := check(ctx); err != nil {
			http.Error(w, "unhealthy: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

package health

import (
	"context"
	"net/http"
	"time"
)

// Check reports whether a dependency is healthy.
type Check func(ctx context.Context) error

// Handler is an additional route mounted on the health server (e.g. /metrics).
type Handler struct {
	Path    string
	Handler http.Handler
}

type Server struct {
	addr     string
	checks   []Check
	timeout  time.Duration
	handlers []Handler
}

func NewServer(addr string, checks ...Check) *Server {
	return &Server{
		addr:    addr,
		checks:  checks,
		timeout: 2 * time.Second,
	}
}

// WithHandler mounts an extra route on the same mux (e.g. /metrics).
func (s *Server) WithHandler(path string, h http.Handler) *Server {
	s.handlers = append(s.handlers, Handler{Path: path, Handler: h})
	return s
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	for _, h := range s.handlers {
		mux.Handle(h.Path, h.Handler)
	}

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

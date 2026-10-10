package wexquota

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var global struct {
	sync.Once
	manager *Manager
}

// Environment returns nil when disabled or unavailable. Reserved paid emails fail closed.
func Environment() *Manager {
	global.Do(func() {
		dir := os.Getenv("WEX_QUOTA_DIR")
		if dir == "" {
			return
		}
		m, e := Open(dir)
		if e != nil {
			return
		}
		if e = m.Serve(); e != nil {
			m.Close()
			return
		}
		global.manager = m
	})
	return global.manager
}
func Managed(email string) bool { return strings.HasPrefix(email, Prefix) }
func (m *Manager) Serve() error {
	path := filepath.Join(m.dir, "control.sock")
	if info, e := os.Lstat(path); e == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return ErrDenied
		}
		if e = os.Remove(path); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	ln, e := net.Listen("unix", path)
	if e != nil {
		return e
	}
	if e = os.Chmod(path, 0600); e != nil {
		ln.Close()
		return e
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.State())
	})
	mux.HandleFunc("POST /v1/disarm", func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > 0 {
			http.Error(w, "managed transport unavailable", 400)
			return
		}
		m.Disarm()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.State())
	})
	mux.HandleFunc("PUT /v1/grants", func(w http.ResponseWriter, r *http.Request) {
		var req Request
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
		dec.DisallowUnknownFields()
		if dec.Decode(&req) != nil {
			http.Error(w, "managed transport unavailable", 400)
			return
		}
		var extra interface{}
		if dec.Decode(&extra) != io.EOF {
			http.Error(w, "managed transport unavailable", 400)
			return
		}
		if m.Apply(req) != nil {
			http.Error(w, "managed transport unavailable", 409)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.State())
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 4096}
	go func() { <-m.stop; srv.Close(); ln.Close() }()
	go srv.Serve(ln)
	go m.sweep()
	return nil
}

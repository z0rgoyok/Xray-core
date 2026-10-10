package splithttp

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

type stalledResponse struct {
	entered chan struct{}
	cancel  chan struct{}
	once    sync.Once
}

func (s *stalledResponse) Header() http.Header { return http.Header{} }
func (s *stalledResponse) WriteHeader(int)     {}
func (s *stalledResponse) Write([]byte) (int, error) {
	close(s.entered)
	<-s.cancel
	return 0, errors.New("deadline")
}
func (s *stalledResponse) Flush() {}
func (s *stalledResponse) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() && !time.Now().Before(t) {
		s.once.Do(func() { close(s.cancel) })
	}
	return nil
}
func TestQuotaCloseCancelsStalledResponseWithoutMutex(t *testing.T) {
	w := &stalledResponse{entered: make(chan struct{}), cancel: make(chan struct{})}
	c := &httpServerConn{Instance: done.New(), ResponseWriter: w}
	finished := make(chan struct{})
	go func() { c.Write([]byte("pending")); close(finished) }()
	<-w.entered
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("revoke deadlocked behind downstream Write")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Write remained blocked")
	}
}

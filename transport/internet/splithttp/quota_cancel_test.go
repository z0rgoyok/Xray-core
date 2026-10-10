package splithttp

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

type stalledResponse struct {
	entered  chan struct{}
	cancel   chan struct{}
	returned chan struct{}
	cleanup  chan struct{}
	once     sync.Once
}

func (s *stalledResponse) Header() http.Header { return http.Header{} }
func (s *stalledResponse) WriteHeader(int)     {}
func (s *stalledResponse) Write([]byte) (int, error) {
	close(s.entered)
	defer close(s.returned)
	<-s.cancel
	<-s.cleanup
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
	w := &stalledResponse{entered: make(chan struct{}), cancel: make(chan struct{}), returned: make(chan struct{}), cleanup: make(chan struct{})}
	c := &httpServerConn{Instance: done.New(), ResponseWriter: w}
	finished := make(chan struct{})
	go func() { c.Write([]byte("pending")); close(finished) }()
	<-w.entered
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	<-w.cancel
	select {
	case <-c.Wait():
		close(w.cleanup)
		t.Fatal("ServeHTTP returned while writer cleanup remained active")
	case <-time.After(20 * time.Millisecond):
	}
	close(w.cleanup)
	select {
	case <-c.Wait():
		select {
		case <-w.returned:
		default:
			t.Fatal("ServeHTTP signaled before concurrent Write finished")
		}
	case <-time.After(time.Second):
		t.Fatal("revoke deadlocked behind downstream Write")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Write remained blocked")
	}
}

// The real HTTP/2 writer clears its pooled state when ServeHTTP returns.
// Any later ResponseController deadline call dereferences that cleared state.
type returnedHandlerWriter struct {
	done          bool
	deadlineCalls int
}

func (w *returnedHandlerWriter) Header() http.Header         { return http.Header{} }
func (w *returnedHandlerWriter) WriteHeader(int)             {}
func (w *returnedHandlerWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *returnedHandlerWriter) Flush()                      {}
func (w *returnedHandlerWriter) SetWriteDeadline(time.Time) error {
	if w.done {
		panic("deadline touched ResponseWriter after handler return")
	}
	w.deadlineCalls++
	return nil
}
func TestDeadlineResetAndRepeatedCloseAfterHandlerReturn(t *testing.T) {
	writer := &returnedHandlerWriter{}
	c := &httpServerConn{Instance: done.New(), ResponseWriter: writer}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	<-c.Wait()
	writer.done = true
	// quotaWriter resets its deadline after a concurrently revoked Write returns.
	if e := c.SetWriteDeadline(time.Time{}); !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal("closed stream deadline was not rejected", e)
	}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	if writer.deadlineCalls != 1 {
		t.Fatal("closed writer called again", writer.deadlineCalls)
	}
}

func TestDeadlineResetCannotClearCloseInterrupt(t *testing.T) {
	writer := &stalledResponse{entered: make(chan struct{}), cancel: make(chan struct{}), returned: make(chan struct{}), cleanup: make(chan struct{})}
	c := &httpServerConn{Instance: done.New(), ResponseWriter: writer}
	writeDone := make(chan struct{})
	go func() { c.Write([]byte("pending")); close(writeDone) }()
	<-writer.entered
	closeDone := make(chan struct{})
	go func() { c.Close(); close(closeDone) }()
	<-writer.cancel
	if c.Done() {
		close(writer.cleanup)
		t.Fatal("Close completed while writer cleanup still active")
	}
	if e := c.SetWriteDeadline(time.Time{}); !errors.Is(e, io.ErrClosedPipe) {
		close(writer.cleanup)
		t.Fatal("deadline reset cleared a close interrupt", e)
	}
	close(writer.cleanup)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close hung")
	}
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("Write hung")
	}
}

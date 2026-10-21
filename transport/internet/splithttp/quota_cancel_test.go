package splithttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/common/wexquota"
)

type discardCloser struct{}

func (discardCloser) Write(b []byte) (int, error) { return len(b), nil }
func (discardCloser) Close() error                { return nil }

func TestExhaustedUploadKeepsOtherUsageAndPolicyAvailable(t *testing.T) {
	dir := t.TempDir()
	id, err := wexquota.Initialize(dir)
	if err != nil {
		t.Fatal(err)
	}
	m, err := wexquota.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	now := time.Now().UTC()
	r := wexquota.Request{Revision: 1, ExpectedLedgerID: id, Grants: []wexquota.Grant{
		{Email: "wex-paid:trial", PackageID: "happ-trial-1", PeriodID: "period", StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour), LimitBytes: 1},
		{Email: "wex-paid:other", PackageID: "happ-2", PeriodID: "period", StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour), LimitBytes: 100},
	}}
	if err := m.Apply(r); err != nil {
		t.Fatal(err)
	}
	q := NewUploadQueue(1)
	if err := q.Push(Packet{Seq: 0, Payload: []byte("queued")}); err != nil {
		t.Fatal(err)
	}
	pushed := make(chan error, 1)
	go func() { pushed <- q.Push(Packet{Seq: 1, Payload: []byte("waiting")}) }()
	select {
	case <-pushed:
		t.Fatal("queue not full")
	case <-time.After(30 * time.Millisecond):
	}
	conn := &splitConn{reader: q, writer: discardCloser{}}
	_, exhausted, err := m.Begin(context.Background(), "wex-paid:trial", conn)
	if err != nil {
		t.Fatal(err)
	}
	b := buf.New()
	b.Write([]byte("x"))
	if err := exhausted.WrapWriter(buf.NewWriter(io.Discard), false, conn).WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
		t.Fatal(err)
	}
	applied := make(chan error, 1)
	go func() { applied <- m.Apply(r) }()
	select {
	case err := <-applied:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		q.Read(make([]byte, 32))
		<-pushed
		<-applied
		t.Fatal("one exhausted upload blocked the next policy/statistics cycle")
	}
	select {
	case err := <-pushed:
		if err == nil {
			t.Fatal("pending upload survived quota close")
		}
	case <-time.After(time.Second):
		q.Close()
		t.Fatal("pending upload was not released")
	}
	otherConn := &splitConn{reader: io.NopCloser(nilReader{}), writer: discardCloser{}}
	_, other, err := m.Begin(context.Background(), "wex-paid:other", otherConn)
	if err != nil {
		t.Fatal(err)
	}
	b = buf.New()
	b.Write([]byte("other"))
	if err := other.WrapWriter(buf.NewWriter(io.Discard), false, otherConn).WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
		t.Fatal(err)
	}
	state := m.State()
	if !state.Healthy {
		t.Fatal("one cap disarmed all clients")
	}
	usage := map[string]int64{}
	for _, u := range state.Usage {
		usage[u.PackageID] = u.UsedBytes
	}
	if usage["happ-trial-1"] != 1 || usage["happ-2"] != 5 {
		t.Fatal("usage lost", usage)
	}
	if _, _, err := m.Begin(context.Background(), "wex-paid:trial", conn); err == nil {
		t.Fatal("exhausted client admitted")
	}
	other.Close()
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }

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

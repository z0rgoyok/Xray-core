package wexquota

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

const MaxInflightBuffers = 128
const WriteTimeout = 10 * time.Second

type contextKey struct{}
type Session struct {
	manager   *Manager
	email     string
	bucket    string
	packageID string
	ctx       context.Context
	cancel    context.CancelFunc
	conn      net.Conn
	once      sync.Once
	tickets   chan struct{}
}

func FromContext(ctx context.Context) *Session { s, _ := ctx.Value(contextKey{}).(*Session); return s }
func (m *Manager) Begin(ctx context.Context, email string, conn net.Conn) (context.Context, *Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.grants[email]
	now := time.Now()
	k := key(g.PackageID, g.PeriodID)
	if !ok || !m.healthy || !m.armed || m.closed || now.Before(g.StartsAt) || !now.Before(g.EndsAt) || m.usage[k].UsedBytes >= g.LimitBytes {
		return ctx, nil, ErrDenied
	}
	ctx, cancel := context.WithCancel(ctx)
	tickets := m.tickets[g.PackageID]
	if tickets == nil {
		tickets = make(chan struct{}, MaxInflightBuffers)
		m.tickets[g.PackageID] = tickets
	}
	s := &Session{manager: m, email: email, bucket: k, packageID: g.PackageID, ctx: ctx, cancel: cancel, conn: conn, tickets: tickets}
	m.sessions[s] = struct{}{}
	return context.WithValue(ctx, contextKey{}, s), s, nil
}
func (s *Session) Close() {
	s.once.Do(func() {
		s.cancel()
		s.conn.SetWriteDeadline(time.Now())
		s.conn.Close()
		s.manager.mu.Lock()
		delete(s.manager.sessions, s)
		s.manager.mu.Unlock()
	})
}
func (s *Session) deadline() (time.Time, error) {
	m := s.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.grants[s.email]
	now := time.Now()
	if !ok || !m.armed || !m.healthy || m.closed || key(g.PackageID, g.PeriodID) != s.bucket || now.Before(g.StartsAt) || !now.Before(g.EndsAt) {
		return time.Time{}, ErrDenied
	}
	d := now.Add(WriteTimeout)
	if g.EndsAt.Before(d) {
		d = g.EndsAt
	}
	return d, nil
}
func (s *Session) charge(n int64, datagram bool) (int64, error) {
	if n <= 0 || n > MaxBlock {
		return 0, ErrDenied
	}
	m := s.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-s.ctx.Done():
		return 0, ErrDenied
	default:
	}
	g, ok := m.grants[s.email]
	now := time.Now()
	if !ok || !m.armed || !m.healthy || m.closed || key(g.PackageID, g.PeriodID) != s.bucket || now.Before(g.StartsAt) || !now.Before(g.EndsAt) {
		return 0, ErrDenied
	}
	u := m.usage[s.bucket]
	remaining := g.LimitBytes - u.UsedBytes
	if remaining <= 0 || (datagram && remaining < n) {
		return 0, ErrDenied
	}
	if n > remaining {
		n = remaining
	}
	u.UsedBytes += n
	if e := m.appendLocked(record{Kind: "charge", Sequence: m.data.Sequence + 1, Usage: &u}); e != nil {
		return 0, e
	}
	m.data.Sequence++
	m.usage[s.bucket] = u
	if e := m.compactLocked(); e != nil {
		m.healthy = false
		go m.closeAll()
		return 0, ErrDenied
	}
	if !time.Now().Before(g.EndsAt) {
		return 0, ErrDenied
	}
	return n, nil
}
func (s *Session) WrapWriter(writer buf.Writer, datagram bool, conn net.Conn) buf.Writer {
	return &quotaWriter{Writer: writer, session: s, datagram: datagram, conn: conn}
}

type quotaWriter struct {
	buf.Writer
	session  *Session
	datagram bool
	conn     net.Conn
}

func (w *quotaWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	s := w.session
	for len(mb) > 0 {
		var chunk buf.MultiBuffer
		if w.datagram {
			var b *buf.Buffer
			mb, b = buf.SplitFirst(mb)
			chunk = buf.MultiBuffer{b}
		} else {
			mb, chunk = buf.SplitSize(mb, MaxBlock)
		}
		select {
		case s.tickets <- struct{}{}:
		case <-s.ctx.Done():
			buf.ReleaseMulti(chunk)
			buf.ReleaseMulti(mb)
			return ErrDenied
		}
		n, e := s.charge(int64(chunk.Len()), w.datagram)
		if e != nil {
			<-s.tickets
			buf.ReleaseMulti(chunk)
			buf.ReleaseMulti(mb)
			s.Close()
			return e
		}
		if n < int64(chunk.Len()) {
			rest, head := buf.SplitSize(chunk, int32(n))
			// SplitSize preserves whole later buffers and can stop before the exact
			// remainder. Preserve the prefix of the next buffer for TCP's final bytes.
			missing := int32(n) - head.Len()
			if missing > 0 && len(rest) > 0 {
				partial := buf.New()
				partial.Write(rest[0].BytesTo(missing))
				head = append(head, partial)
			}
			buf.ReleaseMulti(rest)
			chunk = head
		}
		d, e := s.deadline()
		if e == nil {
			e = w.conn.SetWriteDeadline(d)
		}
		if e == nil {
			e = w.Writer.WriteMultiBuffer(chunk)
		} else {
			buf.ReleaseMulti(chunk)
		}
		w.conn.SetWriteDeadline(time.Time{})
		<-s.tickets
		if e != nil {
			buf.ReleaseMulti(mb)
			s.Close()
			return e
		}
	}
	return nil
}
func (w *quotaWriter) Close() error { return common.Close(w.Writer) }
func (w *quotaWriter) Interrupt()   { common.Interrupt(w.Writer) }
func (m *Manager) sweep() {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
		}
		m.mu.Lock()
		var closed []*Session
		now := time.Now()
		for s := range m.sessions {
			g, ok := m.grants[s.email]
			if !ok || !m.armed || !m.healthy || !now.Before(g.EndsAt) || key(g.PackageID, g.PeriodID) != s.bucket || m.usage[s.bucket].UsedBytes >= g.LimitBytes {
				closed = append(closed, s)
			}
		}
		m.mu.Unlock()
		for _, s := range closed {
			s.Close()
		}
	}
}

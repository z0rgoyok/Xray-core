package wexquota

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/session"
)

func fixture(t *testing.T, limit int64) (*Manager, Request) {
	t.Helper()
	dir := t.TempDir()
	id, e := Initialize(dir)
	if e != nil {
		t.Fatal(e)
	}
	m, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	now := time.Now().UTC()
	r := Request{Revision: 1, ExpectedLedgerID: id, Grants: []Grant{{Email: Prefix + "test", PackageID: "p", PeriodID: "2026-10-10T12:00:00Z", StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour), LimitBytes: limit}}}
	if e = m.Apply(r); e != nil {
		t.Fatal(e)
	}
	return m, r
}
func flow(t *testing.T, m *Manager, email string) *Session {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { b.Close() })
	_, s, e := m.Begin(context.Background(), email, a)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	return s
}

func TestTrialContractUpgradePreservesConsumptionAndRestart(t *testing.T) {
	m, r := fixture(t, 1_000_000_000)
	r.Revision++
	r.Grants[0].PackageID = "happ-trial-42"
	if err := m.Apply(r); err != nil {
		t.Fatal(err)
	}
	s := flow(t, m, r.Grants[0].Email)
	if n, err := s.charge(23, false); err != nil || n != 23 {
		t.Fatal(n, err)
	}
	before := m.State()
	r.Revision++
	r.Grants[0].LimitBytes = 30_000_000_000
	if err := m.Apply(r); err == nil {
		t.Fatal("implicit quota increase accepted")
	}
	if m.State().Sequence != before.Sequence {
		t.Fatal("refused request changed ledger")
	}
	r.TrialQuotaVersion = 2
	if err := m.Apply(r); err != nil {
		t.Fatal(err)
	}
	if n, err := s.charge(11, false); err != nil || n != 11 {
		t.Fatal("existing flow lost", n, err)
	}
	for _, u := range m.State().Usage {
		if u.PackageID == "happ-trial-42" && u.UsedBytes != 34 {
			t.Fatal(u)
		}
	}
	if err := m.Apply(r); err != nil {
		t.Fatal("idempotent retry", err)
	}
	dir := m.dir
	s.Close()
	m.Close()
	m, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Apply(r); err != nil {
		t.Fatal("restart", err)
	}
	for _, u := range m.State().Usage {
		if u.PackageID == "happ-trial-42" && u.UsedBytes != 34 {
			t.Fatal("usage reset", u)
		}
	}
	r.Revision++
	r.Grants[0].LimitBytes = 1_000_000_000
	if err := m.Apply(r); err == nil {
		t.Fatal("downgrade accepted")
	}
}

func TestTrialUpgradeCannotChangePaidOrArbitraryQuota(t *testing.T) {
	for _, packageID := range []string{"happ-42", "happ-trial-0", "happ-trial-42-other"} {
		t.Run(packageID, func(t *testing.T) {
			m, r := fixture(t, 1_000_000_000)
			r.Revision++
			r.Grants[0].PackageID = packageID
			if err := m.Apply(r); err != nil {
				t.Fatal(err)
			}
			before := m.State()
			r.Revision++
			r.TrialQuotaVersion = 2
			r.Grants[0].LimitBytes = 30_000_000_000
			if err := m.Apply(r); err == nil {
				t.Fatal("foreign package upgraded")
			}
			if m.State().Sequence != before.Sequence {
				t.Fatal("refusal changed ledger")
			}
		})
	}
}
func TestDurableLimitRotationAndRestart(t *testing.T) {
	m, r := fixture(t, 100)
	s := flow(t, m, Prefix+"test")
	if n, e := s.charge(70, false); n != 70 || e != nil {
		t.Fatal(n, e)
	}
	r.Revision++
	r.Grants[0].Email = Prefix + "replacement"
	if e := m.Apply(r); e != nil {
		t.Fatal(e)
	}
	if _, e := s.charge(1, false); e == nil {
		t.Fatal("old credential forwarded")
	}
	fresh := flow(t, m, Prefix+"replacement")
	if n, e := fresh.charge(50, false); n != 30 || e != nil {
		t.Fatal(n, e)
	}
	if _, e := fresh.charge(1, false); e == nil {
		t.Fatal("overshoot")
	}
	dir := m.dir
	high := m.State()
	m.Close()
	m, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	if m.State().Healthy {
		t.Fatal("restart must await external highwater")
	}
	r.MinSequence = high.Sequence
	if e = m.Apply(r); e != nil {
		t.Fatal(e)
	}
	if got := m.State().Usage[0].UsedBytes; got != 100 {
		t.Fatal(got)
	}
	if _, _, e := m.Begin(context.Background(), Prefix+"replacement", nil); e == nil {
		t.Fatal("exhausted user reopened")
	}
}
func TestParallelNoOvershootAndNoFlowCap(t *testing.T) {
	m, _ := fixture(t, 10000)
	sessions := make([]*Session, 160)
	for i := range sessions {
		sessions[i] = flow(t, m, Prefix+"test")
	}
	var wg sync.WaitGroup
	for _, s := range sessions {
		wg.Go(func() {
			for i := 0; i < 30; i++ {
				s.charge(7, false)
			}
		})
	}
	wg.Wait()
	if n := m.State().Usage[0].UsedBytes; n != 10000 {
		t.Fatal(n)
	}
}
func TestExpiredWindowAndOtherPackage(t *testing.T) {
	m, r := fixture(t, 100)
	s := flow(t, m, Prefix+"test")
	g := r.Grants[0]
	g.Email = Prefix + "other"
	g.PackageID = "q"
	r.Grants = append(r.Grants, g)
	r.Revision++
	if e := m.Apply(r); e != nil {
		t.Fatal(e)
	}
	other := flow(t, m, g.Email)
	for i := range r.Grants {
		if r.Grants[i].Email == Prefix+"test" {
			r.Grants[i].EndsAt = time.Now().Add(-time.Millisecond)
		}
	}
	r.Revision++
	if e := m.Apply(r); e != nil {
		t.Fatal(e)
	}
	if _, e := s.charge(1, false); e == nil {
		t.Fatal("after expiry")
	}
	if _, e := other.charge(1, false); e != nil {
		t.Fatal("cross user", e)
	}
}
func TestWALFailureClosesPaidAndChargesNotForwarded(t *testing.T) {
	m, _ := fixture(t, 100)
	s := flow(t, m, Prefix+"test")
	m.syncFile = func(*os.File) error { return errors.New("injected") }
	if n, e := s.charge(4, false); n != 0 || e == nil {
		t.Fatal(n, e)
	}
	if m.State().Healthy {
		t.Fatal("failure did not fail closed")
	}
}
func TestCorruptGapAndTornTail(t *testing.T) {
	m, _ := fixture(t, 100)
	s := flow(t, m, Prefix+"test")
	s.charge(12, false)
	dir := m.dir
	m.Close()
	f, e := os.OpenFile(filepath.Join(dir, "ledger.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	f.WriteString("{\"partial\"")
	f.Close()
	m, e = Open(dir)
	if e != nil {
		t.Fatal("torn unsynced tail recovery", e)
	}
	if m.State().Usage[0].UsedBytes != 12 {
		t.Fatal("committed bytes lost")
	}
	m.Close()
	f, _ = os.OpenFile(filepath.Join(dir, "ledger.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	b, _ := encode(record{Kind: "charge", Sequence: 999, Usage: &Usage{PackageID: "p", PeriodID: "2026-10-10T12:00:00Z", UsedBytes: 13}})
	f.Write(b)
	f.Close()
	if bad, e := Open(dir); e == nil {
		bad.Close()
		t.Fatal("gap accepted")
	}
}
func TestCompactionAndWriterLock(t *testing.T) {
	m, _ := fixture(t, 100000)
	if x, e := Open(m.dir); e == nil {
		x.Close()
		t.Fatal("two writers")
	}
	s := flow(t, m, Prefix+"test")
	for i := 0; i < 5000; i++ {
		if _, e := s.charge(1, false); e != nil {
			t.Fatal(e)
		}
	}
	info, _ := os.Stat(filepath.Join(m.dir, "ledger.jsonl"))
	if info.Size() > 512<<10 {
		t.Fatal("unbounded WAL", info.Size())
	}
	dir := m.dir
	m.Close()
	m, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	if m.State().Usage[0].UsedBytes != 5000 {
		t.Fatal("compaction lost bytes")
	}
}
func TestFloorsHighwaterAndRevision(t *testing.T) {
	m, r := fixture(t, 100)
	r.Floors = []Usage{{PackageID: "p", PeriodID: r.Grants[0].PeriodID, UsedBytes: 20}}
	if e := m.Apply(r); e != nil {
		t.Fatal(e)
	}
	if m.State().Usage[0].UsedBytes != 20 {
		t.Fatal("floor not restored")
	}
	r.Floors[0].UsedBytes = 1
	if e := m.Apply(r); e != nil {
		t.Fatal(e)
	}
	if m.State().Usage[0].UsedBytes != 20 {
		t.Fatal("floor decreased")
	}
	r.Revision = 0
	if e := m.Apply(r); e == nil {
		t.Fatal("stale accepted")
	}
	r.Revision = 1
	r.MinSequence = m.State().Sequence + 1
	if e := m.Apply(r); e == nil {
		t.Fatal("rollback accepted")
	}
}

type captureWriter struct{ bytes.Buffer }

func (w *captureWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer buf.ReleaseMulti(mb)
	for _, b := range mb {
		w.Write(b.Bytes())
	}
	return nil
}
func TestWriterExactRemainingAndWholeDatagram(t *testing.T) {
	m, _ := fixture(t, 5)
	s := flow(t, m, Prefix+"test")
	out := &captureWriter{}
	w := s.WrapWriter(out, false, s.conn)
	if e := w.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("abcdefg"))}); e != nil {
		t.Fatal(e)
	}
	if out.String() != "abcde" {
		t.Fatal(out.String())
	}
	m2, _ := fixture(t, 5)
	s2 := flow(t, m2, Prefix+"test")
	out2 := &captureWriter{}
	if e := s2.WrapWriter(out2, true, s2.conn).WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("abcdefg"))}); e == nil || out2.Len() != 0 || m2.State().Usage[0].UsedBytes != 0 {
		t.Fatal("partial datagram")
	}
}
func BenchmarkDurableSmallBlocks(b *testing.B) {
	dir := b.TempDir()
	id, _ := Initialize(dir)
	m, e := Open(dir)
	if e != nil {
		b.Fatal(e)
	}
	defer m.Close()
	r := Request{Revision: 1, ExpectedLedgerID: id, Grants: []Grant{{Email: Prefix + "bench", PackageID: "p", PeriodID: "month", StartsAt: time.Now().Add(-time.Hour), EndsAt: time.Now().Add(time.Hour), LimitBytes: 1 << 50}}}
	m.Apply(r)
	a, c := net.Pipe()
	defer c.Close()
	_, s, _ := m.Begin(context.Background(), Prefix+"bench", a)
	defer s.Close()
	b.SetBytes(2048)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, e = s.charge(2048, false); e != nil {
			b.Fatal(e)
		}
	}
}

func TestUnknownZeroFloorIsInertAndPositiveFloorRejectedAtomically(t *testing.T) {
	m, r := fixture(t, 100)
	s := flow(t, m, Prefix+"test")
	s.charge(5, false)
	r.Floors = []Usage{{PackageID: "never-active", PeriodID: "old-window", UsedBytes: 0}}
	if e := m.Apply(r); e != nil {
		t.Fatal(e)
	}
	before := m.State()
	r.Revision++
	r.Floors[0].UsedBytes = 1
	if e := m.Apply(r); e == nil {
		t.Fatal("unknown positive floor accepted")
	}
	after := m.State()
	if after.Sequence != before.Sequence || after.Revision != before.Revision || len(after.Usage) != 1 || after.Usage[0].UsedBytes != 5 {
		t.Fatal("partial mutation", after)
	}
	if _, e := s.charge(1, false); e != nil {
		t.Fatal("invalid snapshot disrupted prior policy", e)
	}
}

func TestAuthenticatedContextSurvivesMuxAndTimeoutOnly(t *testing.T) {
	m, _ := fixture(t, 100)
	a, b := net.Pipe()
	defer b.Close()
	ctx, q, e := m.Begin(context.Background(), Prefix+"test", a)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx = session.ContextWithTimeoutOnly(session.SubContextFromMuxInbound(ctx), true)
	ctx = session.SubContextFromMuxInbound(ctx)
	if FromContext(ctx) != q {
		t.Fatal("authenticated handle lost through nested mux context")
	}
}

func TestDisarmPreservesHighwaterAndRequiresApply(t *testing.T) {
	m, r := fixture(t, 100)
	s := flow(t, m, Prefix+"test")
	s.charge(10, false)
	before := m.State()
	m.Disarm()
	if m.State().Healthy {
		t.Fatal("disarm still healthy")
	}
	if _, e := s.charge(1, false); e == nil {
		t.Fatal("disarmed forward")
	}
	after := m.State()
	if after.Sequence != before.Sequence || after.Revision != before.Revision || after.Usage[0].UsedBytes != 10 {
		t.Fatal("disarm changed ledger")
	}
	r.MinSequence = before.Sequence
	if e := m.Apply(r); e != nil {
		t.Fatal(e)
	}
	fresh := flow(t, m, Prefix+"test")
	if _, e := fresh.charge(1, false); e != nil {
		t.Fatal(e)
	}
}

func TestTCPRemainingAcrossMultipleBuffers(t *testing.T) {
	m, _ := fixture(t, 3000)
	s := flow(t, m, Prefix+"test")
	out := &captureWriter{}
	input := buf.MultiBuffer{buf.FromBytes(bytes.Repeat([]byte("a"), 2048)), buf.FromBytes(bytes.Repeat([]byte("b"), 2048))}
	if e := s.WrapWriter(out, false, s.conn).WriteMultiBuffer(input); e != nil {
		t.Fatal(e)
	}
	if out.Len() != 3000 || m.State().Usage[0].UsedBytes != 3000 || out.Bytes()[2048] != 'b' {
		t.Fatal("TCP remaining lost", out.Len(), m.State())
	}
}

func TestNamedInitializationPinsIdentityAndNeverReplacesLedger(t *testing.T) {
	dir := t.TempDir()
	id := "aabbccddaabbccddaabbccddaabbccdd"
	got, e := InitializeNamed(dir, id)
	if e != nil || got != id {
		t.Fatal(got, e)
	}
	m, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if m.State().LedgerID != id {
		t.Fatal("identity changed")
	}
	m.Close()
	if _, e = InitializeNamed(dir, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"); e == nil {
		t.Fatal("replaced existing ledger")
	}
	if _, e = InitializeNamed(t.TempDir(), "not-an-id"); e == nil {
		t.Fatal("malformed identity accepted")
	}
}

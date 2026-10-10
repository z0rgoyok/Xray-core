// Package wexquota implements an opt-in, durable payload quota for managed VLESS users.
package wexquota

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const Prefix = "wex-paid:"
const MaxBlock = 65536
const compactEvery = 4096

var ErrDenied = errors.New("managed transport unavailable")

type Grant struct {
	Email      string    `json:"email"`
	PackageID  string    `json:"packageId"`
	PeriodID   string    `json:"periodId"`
	StartsAt   time.Time `json:"startsAt"`
	EndsAt     time.Time `json:"endsAt"`
	LimitBytes int64     `json:"limitBytes"`
}
type Usage struct {
	PackageID string `json:"packageId"`
	PeriodID  string `json:"periodId"`
	UsedBytes int64  `json:"usedBytes"`
}
type Request struct {
	Revision         uint64  `json:"revision"`
	ExpectedLedgerID string  `json:"expectedLedgerId"`
	MinSequence      uint64  `json:"minSequence"`
	Grants           []Grant `json:"grants"`
	Floors           []Usage `json:"floors"`
}
type State struct {
	LedgerID    string  `json:"ledgerId"`
	Sequence    uint64  `json:"sequence"`
	Revision    uint64  `json:"revision"`
	Usage       []Usage `json:"usage"`
	ActiveFlows int     `json:"activeFlows"`
	Healthy     bool    `json:"healthy"`
	ObservedAt  string  `json:"observedAt"`
}
type snapshot struct {
	LedgerID    string           `json:"ledgerId"`
	Sequence    uint64           `json:"sequence"`
	Revision    uint64           `json:"revision"`
	RequestHash string           `json:"requestHash"`
	Grants      []Grant          `json:"grants"`
	Usage       []Usage          `json:"usage"`
	Limits      map[string]int64 `json:"limits"`
}
type record struct {
	Kind     string    `json:"kind"`
	Sequence uint64    `json:"sequence"`
	Snapshot *snapshot `json:"snapshot,omitempty"`
	Usage    *Usage    `json:"usage,omitempty"`
}
type envelope struct {
	Record json.RawMessage `json:"record"`
	SHA256 string          `json:"sha256"`
}
type Manager struct {
	mu       sync.Mutex
	dir      string
	file     *os.File
	lock     *os.File
	data     snapshot
	grants   map[string]Grant
	usage    map[string]Usage
	sessions map[*Session]struct{}
	healthy  bool
	armed    bool
	tickets  map[string]chan struct{}
	closed   bool
	records  int
	stop     chan struct{}
	syncFile func(*os.File) error
}

func key(packageID, periodID string) string { return packageID + "\x00" + periodID }
func validID(s string) bool                 { return s != "" && len(s) <= 160 && !strings.ContainsAny(s, "\x00\r\n") }
func syncDir(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func encode(r record) ([]byte, error) {
	b, e := json.Marshal(r)
	if e != nil {
		return nil, e
	}
	sum := sha256.Sum256(b)
	out, e := json.Marshal(envelope{b, hex.EncodeToString(sum[:])})
	return append(out, '\n'), e
}
func Initialize(dir string) (string, error) {
	if !filepath.IsAbs(dir) {
		return "", ErrDenied
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return "", e
	}
	l, e := openLocked(dir)
	if e != nil {
		return "", e
	}
	defer l.Close()
	f, e := os.OpenFile(filepath.Join(dir, "ledger.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return "", e
	}
	defer f.Close()
	var id [16]byte
	if _, e = rand.Read(id[:]); e != nil {
		return "", e
	}
	s := snapshot{LedgerID: hex.EncodeToString(id[:]), Limits: map[string]int64{}, Grants: []Grant{}, Usage: []Usage{}}
	b, e := encode(record{Kind: "snapshot", Snapshot: &s})
	if e != nil {
		return "", e
	}
	if _, e = f.Write(b); e != nil {
		return "", e
	}
	if e = f.Sync(); e != nil {
		return "", e
	}
	if e = syncDir(dir); e != nil {
		return "", e
	}
	return s.LedgerID, nil
}
func Open(dir string) (*Manager, error) {
	m := &Manager{dir: dir, grants: map[string]Grant{}, usage: map[string]Usage{}, sessions: map[*Session]struct{}{}, tickets: map[string]chan struct{}{}, healthy: true, stop: make(chan struct{}), syncFile: func(f *os.File) error { return f.Sync() }}
	if !filepath.IsAbs(dir) {
		return nil, ErrDenied
	}
	var e error
	m.lock, e = openLocked(dir)
	if e != nil {
		return nil, e
	}
	fail := func(e error) (*Manager, error) {
		if m.file != nil {
			m.file.Close()
		}
		m.lock.Close()
		return nil, e
	}
	path := filepath.Join(dir, "ledger.jsonl")
	info, e := os.Lstat(path)
	if e != nil {
		return fail(e)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return fail(ErrDenied)
	}
	m.file, e = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
	if e != nil {
		return fail(e)
	}
	rd := bufio.NewReader(m.file)
	var offset int64
	for {
		line, err := rd.ReadBytes('\n')
		if err == io.EOF && len(line) == 0 {
			break
		}
		if err == io.EOF && len(line) > 0 {
			if m.file.Truncate(offset) != nil || m.file.Sync() != nil {
				return fail(ErrDenied)
			}
			break
		}
		if err != nil {
			return fail(ErrDenied)
		}
		offset += int64(len(line))
		if len(line) > 8<<20 {
			return fail(ErrDenied)
		}
		var env envelope
		if json.Unmarshal(line, &env) != nil {
			return fail(ErrDenied)
		}
		sum := sha256.Sum256(env.Record)
		if env.SHA256 != hex.EncodeToString(sum[:]) {
			return fail(ErrDenied)
		}
		var r record
		if json.Unmarshal(env.Record, &r) != nil {
			return fail(ErrDenied)
		}
		if r.Kind == "snapshot" && r.Snapshot != nil {
			if m.records != 0 && r.Sequence != m.data.Sequence+1 {
				return fail(ErrDenied)
			}
			if r.Snapshot.Sequence != r.Sequence || !validID(r.Snapshot.LedgerID) {
				return fail(ErrDenied)
			}
			m.data = *r.Snapshot
			m.grants = map[string]Grant{}
			m.usage = map[string]Usage{}
			for _, g := range m.data.Grants {
				m.grants[g.Email] = g
			}
			for _, u := range m.data.Usage {
				m.usage[key(u.PackageID, u.PeriodID)] = u
			}
		} else if r.Kind == "charge" && r.Usage != nil && m.records != 0 {
			k := key(r.Usage.PackageID, r.Usage.PeriodID)
			old, ok := m.usage[k]
			if !ok || r.Sequence != m.data.Sequence+1 || r.Usage.UsedBytes < old.UsedBytes || r.Usage.UsedBytes > m.data.Limits[k] {
				return fail(ErrDenied)
			}
			m.data.Sequence = r.Sequence
			m.usage[k] = *r.Usage
		} else {
			return fail(ErrDenied)
		}
		m.records++
	}
	if m.records == 0 || m.data.Limits == nil {
		return fail(ErrDenied)
	}
	return m, nil
}
func (m *Manager) snapshotLocked() snapshot {
	s := m.data
	s.Grants = make([]Grant, 0, len(m.grants))
	for _, g := range m.grants {
		s.Grants = append(s.Grants, g)
	}
	sort.Slice(s.Grants, func(i, j int) bool { return s.Grants[i].Email < s.Grants[j].Email })
	s.Usage = make([]Usage, 0, len(m.usage))
	for _, u := range m.usage {
		s.Usage = append(s.Usage, u)
	}
	sort.Slice(s.Usage, func(i, j int) bool {
		return key(s.Usage[i].PackageID, s.Usage[i].PeriodID) < key(s.Usage[j].PackageID, s.Usage[j].PeriodID)
	})
	s.Limits = make(map[string]int64, len(m.data.Limits))
	for k, v := range m.data.Limits {
		s.Limits[k] = v
	}
	return s
}
func (m *Manager) appendLocked(r record) error {
	if !m.healthy || m.closed {
		return ErrDenied
	}
	b, e := encode(r)
	if e == nil {
		var n int
		n, e = m.file.Write(b)
		if e == nil && n != len(b) {
			e = io.ErrShortWrite
		}
	}
	if e == nil {
		e = m.syncFile(m.file)
	}
	if e != nil {
		m.healthy = false
		go m.closeAll()
		return ErrDenied
	}
	m.records++
	return nil
}
func (m *Manager) compactLocked() error {
	if m.records < compactEvery {
		return nil
	}
	s := m.snapshotLocked()
	b, e := encode(record{Kind: "snapshot", Sequence: s.Sequence, Snapshot: &s})
	if e != nil {
		return e
	}
	tmp := filepath.Join(m.dir, "checkpoint.tmp")
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(e) {
		if e = os.Remove(tmp); e != nil {
			return e
		}
		f, e = os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	}
	if e != nil {
		return e
	}
	defer os.Remove(tmp)
	if _, e = f.Write(b); e == nil {
		e = m.syncFile(f)
	}
	if ce := f.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(tmp, filepath.Join(m.dir, "ledger.jsonl")); e != nil {
		return e
	}
	if e = syncDir(m.dir); e != nil {
		return e
	}
	nf, e := os.OpenFile(filepath.Join(m.dir, "ledger.jsonl"), os.O_RDWR|os.O_APPEND, 0600)
	if e != nil {
		return e
	}
	m.file.Close()
	m.file = nf
	m.records = 1
	return nil
}
func (m *Manager) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.snapshotLocked()
	return State{LedgerID: s.LedgerID, Sequence: s.Sequence, Revision: s.Revision, Usage: s.Usage, ActiveFlows: len(m.sessions), Healthy: m.healthy && m.armed && !m.closed, ObservedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z")}
}
func (m *Manager) Apply(req Request) error {
	m.mu.Lock()
	finish := func(e error) error { m.mu.Unlock(); return e }
	if req.ExpectedLedgerID != m.data.LedgerID || req.MinSequence > m.data.Sequence {
		m.armed = false
		m.mu.Unlock()
		m.closeAll()
		return ErrDenied
	}
	if !m.healthy || m.closed || req.Revision == 0 {
		return finish(ErrDenied)
	}
	sort.Slice(req.Grants, func(i, j int) bool { return req.Grants[i].Email < req.Grants[j].Email })
	sort.Slice(req.Floors, func(i, j int) bool {
		return key(req.Floors[i].PackageID, req.Floors[i].PeriodID) < key(req.Floors[j].PackageID, req.Floors[j].PeriodID)
	})
	raw, _ := json.Marshal(struct {
		Revision uint64
		Grants   []Grant
	}{req.Revision, req.Grants})
	hash := sha256.Sum256(raw)
	h := hex.EncodeToString(hash[:])
	if req.Revision < m.data.Revision || (req.Revision == m.data.Revision && h != m.data.RequestHash) {
		return finish(ErrDenied)
	}
	if len(req.Grants) > 10000 || len(req.Floors) > 10000 {
		return finish(ErrDenied)
	}
	next := m.snapshotLocked()
	next.Sequence++
	next.Revision = req.Revision
	next.RequestHash = h
	next.Grants = req.Grants
	grants := map[string]Grant{}
	packages := map[string]bool{}
	usage := map[string]Usage{}
	for k, u := range m.usage {
		usage[k] = u
	}
	for _, g := range req.Grants {
		if !strings.HasPrefix(g.Email, Prefix) || !validID(g.Email) || !validID(g.PackageID) || !validID(g.PeriodID) || g.LimitBytes <= 0 || g.LimitBytes > 1<<53 || !g.EndsAt.After(g.StartsAt) || g.StartsAt.IsZero() || packages[g.PackageID] {
			return finish(ErrDenied)
		}
		if _, ok := grants[g.Email]; ok {
			return finish(ErrDenied)
		}
		packages[g.PackageID] = true
		grants[g.Email] = g
		k := key(g.PackageID, g.PeriodID)
		if limit, ok := next.Limits[k]; ok && limit != g.LimitBytes {
			return finish(ErrDenied)
		}
		next.Limits[k] = g.LimitBytes
		if _, ok := usage[k]; !ok {
			usage[k] = Usage{PackageID: g.PackageID, PeriodID: g.PeriodID}
		}
	}
	for _, f := range req.Floors {
		k := key(f.PackageID, f.PeriodID)
		u, ok := usage[k]
		if !ok && f.UsedBytes == 0 {
			continue
		}
		if !ok || f.UsedBytes < 0 || f.UsedBytes > next.Limits[k] {
			return finish(ErrDenied)
		}
		if f.UsedBytes > u.UsedBytes {
			usage[k] = f
		}
	}
	next.Usage = make([]Usage, 0, len(usage))
	for _, u := range usage {
		next.Usage = append(next.Usage, u)
	}
	if e := m.appendLocked(record{Kind: "snapshot", Sequence: next.Sequence, Snapshot: &next}); e != nil {
		return finish(e)
	}
	m.data = next
	m.grants = grants
	m.usage = usage
	m.armed = true
	var closeSessions []*Session
	for s := range m.sessions {
		g, ok := grants[s.email]
		if !ok || key(g.PackageID, g.PeriodID) != s.bucket || !time.Now().Before(g.EndsAt) || usage[s.bucket].UsedBytes >= g.LimitBytes {
			closeSessions = append(closeSessions, s)
		}
	}
	if e := m.compactLocked(); e != nil {
		m.healthy = false
		m.mu.Unlock()
		m.closeAll()
		return ErrDenied
	}
	m.mu.Unlock()
	for _, s := range closeSessions {
		s.Close()
	}
	return nil
}
func (m *Manager) closeAll() {
	m.mu.Lock()
	list := make([]*Session, 0, len(m.sessions))
	for s := range m.sessions {
		list = append(list, s)
	}
	m.mu.Unlock()
	for _, s := range list {
		s.Close()
	}
}
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.stop)
	m.mu.Unlock()
	m.closeAll()
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.file.Close()
	m.lock.Close()
	return e
}

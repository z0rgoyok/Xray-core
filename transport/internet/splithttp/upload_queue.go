package splithttp

// upload_queue is a specialized priorityqueue + channel to reorder generic
// packets by a sequence number

import (
	"container/heap"
	"io"
	"sync"

	"github.com/xtls/xray-core/common/errors"
)

type Packet struct {
	Reader  io.ReadCloser
	Payload []byte
	Seq     uint64
}

type uploadQueue struct {
	reader          io.ReadCloser
	readerMutex     sync.Mutex
	readMutex       sync.Mutex
	closeOnce       sync.Once
	closing         chan struct{}
	closeErr        error
	nomore          bool
	pushedPackets   chan Packet
	writeCloseMutex sync.Mutex
	heap            uploadHeap
	nextSeq         uint64
	maxPackets      int
}

func NewUploadQueue(maxPackets int) *uploadQueue {
	return &uploadQueue{
		pushedPackets: make(chan Packet, maxPackets),
		closing:       make(chan struct{}),
		heap:          uploadHeap{},
		nextSeq:       0,
		maxPackets:    maxPackets,
	}
}

func (h *uploadQueue) Push(p Packet) error {
	h.writeCloseMutex.Lock()
	defer h.writeCloseMutex.Unlock()

	select {
	case <-h.closing:
		return errors.New("packet queue closed")
	default:
	}
	if h.nomore {
		return errors.New("h.reader already exists")
	}
	if p.Reader != nil {
		h.nomore = true
	}
	select {
	case <-h.closing:
		return errors.New("packet queue closed")
	case h.pushedPackets <- p:
		return nil
	}
}

func (h *uploadQueue) Close() error {
	h.closeOnce.Do(func() {
		// Wake a producer blocked on capacity BEFORE waiting for its mutex.
		close(h.closing)
		h.writeCloseMutex.Lock()
		defer h.writeCloseMutex.Unlock()
		close(h.pushedPackets)
		for p := range h.pushedPackets {
			if p.Reader != nil {
				h.readerMutex.Lock()
				h.reader = p.Reader
				h.readerMutex.Unlock()
			}
		}
		h.readerMutex.Lock()
		reader := h.reader
		h.readerMutex.Unlock()
		if reader != nil {
			h.closeErr = reader.Close()
		}
	})
	return h.closeErr
}

func (h *uploadQueue) readStream(reader io.ReadCloser, b []byte) (int, error) {
	h.readerMutex.Lock()
	select {
	case <-h.closing:
		h.readerMutex.Unlock()
		reader.Close()
		return 0, io.EOF
	default:
		h.reader = reader
		h.readerMutex.Unlock()
		return reader.Read(b)
	}
}

func (h *uploadQueue) Read(b []byte) (int, error) {
	h.readMutex.Lock()
	defer h.readMutex.Unlock()
	select {
	case <-h.closing:
		return 0, io.EOF
	default:
	}
	h.readerMutex.Lock()
	reader := h.reader
	h.readerMutex.Unlock()
	if reader != nil {
		return reader.Read(b)
	}

	if len(h.heap) == 0 {
		packet, more := <-h.pushedPackets
		if !more {
			return 0, io.EOF
		}
		if packet.Reader != nil {
			return h.readStream(packet.Reader, b)
		}
		heap.Push(&h.heap, packet)
	}

	for len(h.heap) > 0 {
		packet := heap.Pop(&h.heap).(Packet)
		n := 0

		if packet.Seq == h.nextSeq {
			copy(b, packet.Payload)
			n = min(len(b), len(packet.Payload))

			if n < len(packet.Payload) {
				// partial read
				packet.Payload = packet.Payload[n:]
				heap.Push(&h.heap, packet)
			} else {
				h.nextSeq = packet.Seq + 1
			}

			return n, nil
		}

		// misordered packet
		if packet.Seq > h.nextSeq {
			if len(h.heap) > h.maxPackets {
				// the "reassembly buffer" is too large, and we want to
				// constrain memory usage somehow. let's tear down the
				// connection, and hope the application retries.
				return 0, errors.New("packet queue is too large")
			}
			heap.Push(&h.heap, packet)
			packet2, more := <-h.pushedPackets
			if !more {
				return 0, io.EOF
			}
			heap.Push(&h.heap, packet2)
		}
	}

	return 0, nil
}

// heap code directly taken from https://pkg.go.dev/container/heap
type uploadHeap []Packet

func (h uploadHeap) Len() int           { return len(h) }
func (h uploadHeap) Less(i, j int) bool { return h[i].Seq < h[j].Seq }
func (h uploadHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *uploadHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Packet))
}

func (h *uploadHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

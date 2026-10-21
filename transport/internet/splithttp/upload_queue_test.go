package splithttp_test

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

func TestCloseReleasesUploadWaitingOnFullQueue(t *testing.T) {
	q := NewUploadQueue(1)
	common.Must(q.Push(Packet{Seq: 0, Payload: []byte("first")}))
	pushed := make(chan error, 1)
	go func() { pushed <- q.Push(Packet{Seq: 1, Payload: []byte("pending")}) }()
	select {
	case <-pushed:
		t.Fatal("upload did not wait for queue capacity")
	case <-time.After(30 * time.Millisecond):
	}
	closed := make(chan error, 1)
	go func() { closed <- q.Close() }()
	select {
	case err := <-closed:
		common.Must(err)
	case <-time.After(time.Second):
		// Release the faulty version before failing, leaving no blocked worker.
		q.Read(make([]byte, 16))
		<-pushed
		<-closed
		t.Fatal("quota close blocked behind a full upload queue")
	}
	select {
	case err := <-pushed:
		if err == nil {
			t.Fatal("upload accepted after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("pending upload was not cancelled")
	}
	if err := q.Push(Packet{Seq: 2}); err == nil {
		t.Fatal("closed queue accepted upload")
	}
}

func Test_regression_readzero(t *testing.T) {
	q := NewUploadQueue(10)
	q.Push(Packet{
		Payload: []byte("x"),
		Seq:     0,
	})
	buf := make([]byte, 20)
	n, err := q.Read(buf)
	common.Must(err)
	if n != 1 {
		t.Error("n=", n)
	}
}

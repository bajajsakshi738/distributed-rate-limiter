package stats

import (
	"testing"
	"time"
)

func TestRecorderSeconds(t *testing.T) {
	r := NewRecorder()
	base := time.Unix(1_700_000_000, 0)
	for i := 0; i < 5; i++ {
		r.Record(Request{At: base}, true, false)
	}
	r.Record(Request{At: base}, false, true)
	r.Record(Request{At: base.Add(2 * time.Second)}, true, false)

	secs := r.Seconds(base.Add(2*time.Second), 3)
	if len(secs) != 3 {
		t.Fatal(len(secs))
	}
	if secs[0].Allowed != 5 || secs[0].Rejected != 1 || secs[0].Degraded != 1 {
		t.Fatalf("first second %+v", secs[0])
	}
	if secs[1].Allowed != 0 || secs[1].Unix != base.Unix()+1 {
		t.Fatalf("empty second %+v", secs[1])
	}
	if secs[2].Allowed != 1 {
		t.Fatalf("last second %+v", secs[2])
	}
	// Old data is not reported once the ring has wrapped.
	later := base.Add(WindowSeconds * time.Second)
	if s := r.Seconds(later, WindowSeconds); s[0].Allowed != 0 && s[0].Unix == base.Unix() {
		t.Fatal("stale second leaked")
	}
}

func TestRecorderRecent(t *testing.T) {
	r := NewRecorder()
	for i := 0; i < RecentSize+5; i++ {
		r.Record(Request{Status: i}, true, false)
	}
	rec := r.Recent()
	if len(rec) != RecentSize || rec[0].Status != RecentSize+4 || rec[RecentSize-1].Status != 5 {
		t.Fatalf("recent wrong: len=%d first=%d last=%d", len(rec), rec[0].Status, rec[len(rec)-1].Status)
	}
}

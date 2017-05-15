package sccache

import (
	"testing"
	"strings"
	"time"
)

func TestReadRequests(t *testing.T) {
	sample := `1494783546	NGLxoKOvzu4	1
1494783765	NGLxoKOvzu4	2
1494783798	aatr_2MstrI	2
1494783854	5bA7nrdVEqE	4
1494979199	5bA7nrdVEqE	2`
	reader := strings.NewReader(sample)
	d, _ := time.ParseDuration("24h")
	p, f, c := ReadRequests(reader, d)
	if len(p[0].Requests) != 4 {
		t.Error("requests number wrong: ", len(p))
	}
	if len(f) != 3 {
		t.Error("file number wrong: ", len(f))
	}
	if len(c) != 3 {
		t.Error("client number wrong: ", len(c))
	}
	t.Log("periods: ", p)
	t.Log("files: ", f)
	t.Log("clients: ", c)
}
package sccache

import (
	"testing"
	"strings"
	"time"
)

var p []period
var f map[string]*file
var c map[string]*client
var sc []*smallCell

func TestReadRequests(t *testing.T) {
	sample := `1494783546	NGLxoKOvzu4	1
1494783765	NGLxoKOvzu4	2
1494783798	aatr_2MstrI	2
1494783854	5bA7nrdVEqE	4
1494979199	5bA7nrdVEqE	2`
	reader := strings.NewReader(sample)
	d, _ := time.ParseDuration("24h")
	p, f, c = ReadRequests(reader, d)
	if len(p[0].requests) != 4 {
		t.Error("requests number wrong:", len(p))
	}
	if len(f) != 3 {
		t.Error("file number wrong:", len(f))
	}
	if len(c) != 3 {
		t.Error("client number wrong:", len(c))
	}
	if f["5bA7nrdVEqE"].popPrd[2] != 1 {
		t.Error("f.popPrd wrong")
	}
	if f["5bA7nrdVEqE"].popAcm[2] != 2 {
		t.Error("f.popAcm wrong")
	}
	if len(c["2"].popPrd[2]) != 1 {
		t.Error("c.popPrd wrong")
	}
	if len(c["2"].popAcm[2]) != 3 {
		t.Error("c.popAcm wrong")
	}
	if p[0].pop[f["NGLxoKOvzu4"]] != 2 {
		t.Error("p.pop wrong")
	}
	t.Log("periods:", p)
	t.Log("files:", f)
	t.Log("clients:", c)
}

func TestReadClientsAssignment(t *testing.T) {
	sample := `1	4
2`
	reader := strings.NewReader(sample)
	sc = ReadClientsAssignment(reader, c)
	if len(sc) != 2 {
		t.Error("sc number wrong:", len(sc))
	}
	if len(sc[1].popAcm[2]) != 3 {
		t.Error("sc popAcm wrong")
	}
	t.Log("sc:", sc)
	t.Log("sc[1]:", sc[1])
	t.Log("c[2].SmallCell:", c["2"].smallCell)
}

func TestClient_assignTo(t *testing.T) {
	c["2"].assignTo(sc[0])
	if len(sc[0].clients) != 3 {
		t.Error("assign error")
	}
	t.Log("sc[0].Clients:", sc[0].clients)
	t.Log("c[2].SmallCell:", c["2"].smallCell)
}
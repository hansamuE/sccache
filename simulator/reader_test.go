package simulator

import (
	"strings"
	"testing"
	"time"
)

func TestReadConfigs(t *testing.T) {
	j := `
[
	{"is_trained": true, "period_duration": "24h", "cooperation_threshold": 0.06, "test_start_period": 2, "cache_policy": "leastRecentlyUsed", "similarity_formula": "exponential", "files_limit": 0, "file_size": 10, "cache_storage_size": 30},
	{"is_trained": true, "period_duration": "24h", "cooperation_threshold": 0.06, "test_start_period": 2, "cache_policy": "leastRecentlyUsed", "similarity_formula": "exponential", "files_limit": 0, "file_size": 10, "cache_storage_size": 50}
]
`
	readConfigs(strings.NewReader(j))
	t.Log(Configs)
}

func TestReadRequests(t *testing.T) {
	sample := `1494783546	NGLxoKOvzu4	1
1494783765	NGLxoKOvzu4	2
1494783798	aatr_2MstrI	2
1494783854	5bA7nrdVEqE	4
1494979199	5bA7nrdVEqE	2`
	reader := strings.NewReader(sample)
	d, _ := time.ParseDuration("24h")
	readRequests(reader, d)
	if len(periods[0].requests) != 4 {
		t.Error("requests number wrong:", len(periods))
	}
	if len(files) != 3 {
		t.Error("file number wrong:", len(files))
	}
	if len(clients) != 3 {
		t.Error("client number wrong:", len(clients))
	}
	if files["5bA7nrdVEqE"].popularityPeriod[2] != 1 {
		t.Error("f.popularityPeriod wrong")
	}
	if files["5bA7nrdVEqE"].popularityAccumulated[2] != 2 {
		t.Error("f.popularityAccumulated wrong")
	}
	if len(clients["2"].popularityPeriod[2]) != 1 {
		t.Error("c.popularityPeriod wrong")
	}
	if len(clients["2"].popularityAccumulated[2]) != 3 {
		t.Error("c.popularityAccumulated wrong")
	}
	if periods[0].popularities[files["NGLxoKOvzu4"]] != 2 {
		t.Error("p.popularities wrong")
	}
	t.Log("periods:", periods)
	t.Log("files:", files)
	t.Log("clients:", clients)
	t.Log("popularFiles:", periods[2].popularFilesAccumulated)
}

func TestReadClientsAssignment(t *testing.T) {
	sample := `1	4
2`
	reader := strings.NewReader(sample)
	readClientsAssignment(reader)
	if len(smallCells) != 2 {
		t.Error("sc number wrong:", len(smallCells))
	}
	if len(smallCells[1].popularitiesAccumulated[2]) != 3 {
		t.Error("sc popularityAccumulated wrong")
	}
	t.Log("sc:", smallCells)
	t.Log("sc[1]:", smallCells[1])
	t.Log("c[2].SmallCell:", clients["2"].smallCell)
}

func TestClient_assignTo(t *testing.T) {
	clients["2"].assignTo(smallCells[0])
	if len(smallCells[0].clients) != 3 {
		t.Error("assign error")
	}
	t.Log("sc[0].Clients:", smallCells[0].clients)
	t.Log("c[2].SmallCell:", clients["2"].smallCell)
}

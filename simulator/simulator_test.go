package simulator

import "testing"

var (
	csl  cacheStorageList
	stat stats = stats{downloaded: 3, served: 72}
)

func TestCalStat(t *testing.T) {
	stat.calRate()
	t.Log("dlRate:", stat.dlRate)
}

func TestArrangeCooperation(t *testing.T) {
	csl = smallCells.arrangeCooperation(-1, exponential)
	t.Log("cacheStorages:", csl)
}

func TestCsl_assignNewClient(t *testing.T) {
	csl.assignNewClient(clients["1"], f1)
}

func TestSimulate(t *testing.T) {
	periods[0].serve(configs[0], nil)
	periods[2].serve(configs[1], nil)
}

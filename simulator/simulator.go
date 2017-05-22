package simulator

import (
	"fmt"
	"os"
	"sort"
)

var (
	cacheStorages cacheStorageList
	periodNo      int
)

type stats struct {
	downloaded int
	served     int
	dlRate     float64
}

func (s *stats) calRate() {
	s.dlRate = float64(s.downloaded) / float64(s.downloaded+s.served)
}

func (pl periodList) calRate() float64 {
	var dl, sv int
	for _, p := range pl {
		dl += p.downloaded
		sv += p.served
	}
	return float64(dl) / float64(dl+sv)
}

func Simulate() {
	configs, err := os.Open("configs.json")
	if err != nil {
		panic(err)
	}
	defer configs.Close()
	readConfigs(configs)

	for _, c := range Configs {
		requests, err := os.Open("requests.csv")
		if err != nil {
			panic(err)
		}
		defer requests.Close()
		fmt.Println("Read Requests...")
		readRequests(requests, c.PeriodDuration)

		if !c.IsTrained {
			fmt.Println("Done Training")
		}

		clusters, err := os.Open("clusters.csv")
		if err != nil {
			panic(err)
		}
		defer clusters.Close()
		readClientsAssignment(clusters)

		preProcess(c)
		var pl periodList = periods[c.TestStartPeriod:]
		pl.serve(c)
		pl.postProcess()
	}
}

func preProcess(config Config) {
	smallCells.arrangeCooperation(config.CooperationThreshold, config.SimilarityFormula)
	for _, f := range files {
		f.size = config.FileSize
	}
	for _, cs := range cacheStorages {
		cs.size = config.CacheStorageSize
	}
}

func (pl periodList) serve(config Config) {
	fmt.Println("Start Testing With Config:", config)
	for _, p := range pl {
		p.serve(config.CachePolicy, p.popularFiles[:config.FilesLimit])
		p.endPeriod(config.CachePolicy, config.SimilarityFormula)
	}
	fmt.Println("All Periods Tested")
}

func (p *period) serve(cp cachePolicy, fileFilter filePopularityList) {
	periodNo = p.id
	for _, r := range p.requests {
		t, f, c := r.time, r.file, r.client
		if !fileFilter.has(f) {
			continue
		}
		if c.smallCell == nil {
			cacheStorages.assignNewClient(c, f)
			p.newClients = append(p.newClients, c)
		}

		cs := c.smallCell.cacheStorage
		sizeCached, cf := cs.cacheFile(f, cp)
		cf.count++
		cf.lastReq = t
		cs.served += sizeCached
		cs.downloaded += f.size - sizeCached
		p.served += sizeCached
		p.downloaded += f.size - sizeCached
	}
}

func (p *period) endPeriod(cp cachePolicy, fn similarityFormula) {
	p.calRate()
	for _, c := range p.newClients {
		sim := c.calSimilarity(fn)
		mi, ms := -1, 0.0
		for i, s := range sim {
			if s > ms {
				mi, ms = i, s
			}
		}
		if mi == -1 {
			c.assignTo(smallCells.leastClients())
		} else {
			c.assignTo(cacheStorages[mi].smallCells.leastClients())
		}
	}
	fmt.Println("End Period:", p.end)
}

func (pl periodList) postProcess() {
	for _, p := range pl {
		fmt.Println(p.end, "\t", p.dlRate)
	}
}

func (csl cacheStorageList) assignNewClient(c *client, f *file) {
	sclf := csl.smallCellsHasFile(f)
	if len(sclf) != 0 {
		c.assignTo(sclf.leastClients())
	} else {
		c.assignTo(smallCells.leastClients())
	}
}

func (scl smallCellList) leastClients() *smallCell {
	sort.Slice(scl, func(i, j int) bool { return len(scl[i].clients) < len(scl[j].clients) })
	return scl[0]
}

func (scl smallCellList) arrangeCooperation(threshold float64, fn similarityFormula) cacheStorageList {
	group := make([]smallCellList, 0)
	if threshold < 0 {
		for _, sc := range scl {
			group = append(group, smallCellList{sc})
		}
	} else {
		ok := make([]bool, len(scl))
		sim := scl.calSimilarity(fn)
		for i := 0; i < len(scl)-1; i++ {
			if ok[i] {
				continue
			}
			group = append(group, smallCellList{scl[i]})
			ok[i] = true
			for j := i + 1; j < len(scl); j++ {
				if ok[j] {
					continue
				}
				if sim[i][j] >= threshold {
					group[len(group)-1] = append(group[len(group)-1], scl[j])
					ok[j] = true
				}
			}
		}
	}

	cacheStorages = make(cacheStorageList, len(group))
	for i, g := range group {
		cacheStorages[i] = &cacheStorage{smallCells: make(smallCellList, 0)}
		for _, sc := range g {
			sc.assignTo(cacheStorages[i])
		}
	}

	return cacheStorages
}

func (sc *smallCell) assignTo(cs *cacheStorage) {
	ocs := sc.cacheStorage
	if ocs != nil {
		scl := ocs.smallCells
		for i := range scl {
			if scl[i] == sc {
				scl = append(scl[:i], scl[i+1:]...)
			}
		}
	}
	cs.smallCells = append(cs.smallCells, sc)
	sc.cacheStorage = cs

	for pn, fp := range sc.popularitiesAccumulated {
		if len(cs.popAcm)-1 < pn {
			cs.popAcm = append(cs.popAcm, make(popularities))
		}
		for k, v := range fp {
			if ocs != nil {
				ocs.popAcm[pn][k] -= v
			}
			cs.popAcm[pn][k] += v
		}
	}
}

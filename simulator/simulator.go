package simulator

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)

var (
	formula       similarityFormula
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

func Simulate(path string) {
	readConfigsFile(path)

	for i, c := range configs {
		readRequestsFile(path, c.PeriodDuration, c.RequestsColumn, c.RequestsComma)

		for j, cp := range configs[i].ParametersList {
			formula = cp.SimilarityFormula
			if !cp.IsTrained {
				fmt.Println("Clustering...")
				var trainPL periodList = periods[cp.TrainStartPeriod : cp.TrainEndPeriod+1]
				cl, guesses := clustering(trainPL, cp.ClusterNumber)
				writeClusteringResultFiles(path, cl, guesses)
			} else {
				fmt.Println("Read Clustering Model...")
				readClusteringResultFiles(path)
			}

			preProcess(cp)
			var pl periodList = periods[cp.TestStartPeriod:]
			pl.serve(cp)
			pl.postProcess()

			writeResultFile(path, pl, configJSONs[i].ParametersListJSON[j])

			reset()
		}
	}
}

func readConfigsFile(path string) {
	f, err := os.Open(path + "configs.json")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	readConfigs(f)
}

func readRequestsFile(path string, duration time.Duration, column []int, comma string) {
	f, err := os.Open(path + "requests.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Println("Read Requests...")
	readRequests(f, duration, column, comma)
}

func readClusteringResultFiles(path string) {
	model := path + "clustering_model.json"
	f, err := os.Open(path + "clustering_result.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	readClusteringResult(model, f)
}

func writeClusteringResultFiles(path string, cl clientList, guesses []int) {
	clusteringModel.PersistToFile(path + "clustering_model.json")
	f, err := os.Create(path + "clustering_result.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for i, c := range cl {
		f.WriteString(c.id + "\t" + strconv.Itoa(guesses[i]) + "\n")
	}
}

//func readClustersFile(path string) {
//	f, err := os.Open(path + "clusters.csv")
//	if err != nil {
//		panic(err)
//	}
//	defer f.Close()
//	readClientsAssignment(f)
//}

func writeResultFile(path string, pl periodList, cpj parametersJSON) {
	if cpj.ResultFileName == "" {
		cpj.ResultFileName = path + "learn" + strconv.Itoa(cpj.TrainStartPeriod) + "to" + strconv.Itoa(cpj.TrainEndPeriod) + "_" + cpj.SimilarityFormula + "_" + strconv.FormatBool(cpj.IsPeriodSimilarity) + "_" + cpj.CachePolicy + "_" + strconv.Itoa(cpj.FilesLimit) + "_" + strconv.Itoa(cpj.FileSize) + "_" + strconv.Itoa(cpj.CacheStorageSize) + ".csv"
	}
	f, err := os.Create(cpj.ResultFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.WriteString("Download Rate:\n")
	for _, p := range pl {
		f.WriteString(p.end.Format("2006-01-02 15") + "\t" + strconv.FormatFloat(p.dlRate, 'f', 5, 64) + "\n")
	}

	f.WriteString("\nSmall Cells \\ Files\n")
	for _, sc := range smallCells {
		for i, file := range filesList {
			f.WriteString(strconv.Itoa(sc.popularitiesAccumulated[pl[len(pl)-1].id][file]))
			if i != len(filesList)-1 {
				f.WriteString("\t")
			} else {
				f.WriteString(strconv.Itoa(len(sc.clients)) + "\n")
			}
		}
	}

	f.WriteString("\nCooperation:\n")
	for _, cs := range cacheStorages {
		for _, sc := range cs.smallCells {
			f.WriteString(strconv.Itoa(sc.id) + "\t")
		}
		f.WriteString("\n")
	}
}

func preProcess(cp parameters) {
	smallCells.arrangeCooperation(cp.CooperationThreshold, cp.SimilarityFormula)
	for _, f := range files {
		f.size = cp.FileSize
	}
	for _, cs := range cacheStorages {
		cs.size = cp.CacheStorageSize
		cs.space = cs.size
	}
}

func (pl periodList) serve(cp parameters) {
	fmt.Println("Start Testing With Config:", cp)
	for pn, p := range pl {
		filesLimit := cp.FilesLimit
		if filesLimit > len(p.popularFiles) {
			filesLimit = len(p.popularFiles)
		}
		p.serve(cp, p.popularFiles[:filesLimit])
		if cp.IsPeriodSimilarity {
			p.endPeriod(cp, pl[pn+1].popularFiles[:filesLimit])
		} else {
			p.endPeriod(cp, nil)
		}
	}
	fmt.Println("All Periods Tested")
}

func (p *period) serve(cp parameters, filter fileList) {
	periodNo = p.id
	for _, r := range p.requests {
		t, f, c := r.time, r.file, r.client
		if len(filter) != 0 && !filter.has(f) {
			continue
		}
		if c.smallCell == nil {
			if len(c.popularityAccumulated[periodNo-1]) == 0 {
				cacheStorages.assignNewClient(c, f)
				p.newClients = append(p.newClients, c)
			} else {
				c.assign(cp, filter)
			}
		}

		cs := c.smallCell.cacheStorage
		sizeCached, cf := cs.cacheFile(f, cp.CachePolicy)
		cf.count++
		cf.lastReq = t
		cs.served += sizeCached
		cs.downloaded += f.size - sizeCached
		p.served += sizeCached
		p.downloaded += f.size - sizeCached
	}
}

func (p *period) endPeriod(cp parameters, filter fileList) {
	p.calRate()
	for _, c := range p.newClients {
		c.assign(cp, filter)
	}
	fmt.Println("End Period:", p.end)
}

func (pl periodList) postProcess() {
	for _, p := range pl {
		fmt.Println(p.end, "\t", p.dlRate)
	}
}

func reset() {
	for _, c := range clients {
		c.smallCell = nil
	}
	for _, p := range periods {
		p.newClients = make(clientList, 0)
		p.stats = stats{}
	}
}

func (c *client) assign(cp parameters, filter fileList) {
	if cp.IsAssignClustering {
		c.assignWithClusteringModel()
	} else {
		c.assignWithSimilarity(cp.SimilarityFormula, filter)
	}
}

func (c *client) assignWithClusteringModel() {
	guess, err := clusteringModel.Predict(c.getFilePopularity())
	if err != nil {
		panic("prediction error")
	}
	c.assignTo(smallCells[int(guess[0])])
}

func (c *client) assignWithSimilarity(fn similarityFormula, filter fileList) {
	sim := c.calSimilarity(filter)
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

func (csl cacheStorageList) assignNewClient(c *client, f *file) {
	scl := csl.smallCellsHasFile(f)
	if len(scl) != 0 {
		c.assignTo(scl.leastClients())
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
		sim := scl.calSimilarity(nil)
		for i := 0; i < len(scl); i++ {
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
		if len(cs.popularitiesAccumulated)-1 < pn {
			cs.popularitiesAccumulated = append(cs.popularitiesAccumulated, make(popularities))
		}
		for k, v := range fp {
			if ocs != nil {
				ocs.popularitiesAccumulated[pn][k] -= v
			}
			cs.popularitiesAccumulated[pn][k] += v
		}
	}
}

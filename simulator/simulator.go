package simulator

import (
	"fmt"
	"os"
	"sort"
	"strconv"
)

var (
	formula       similarityFormula
	cacheStorages cacheStorageList
	periodNo      int
	newUserNum    []int
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
		readRequestsFile(path, c)

		for j, cp := range configs[i].ParametersList {
			cpj := configJSONs[i].ParametersListJSON[j]
			formula = cp.SimilarityFormula
			if !cp.IsTrained {
				fmt.Println("Clustering...")
				trainEndPeriod := cp.TrainEndPeriod + 1
				if trainEndPeriod > len(periods) {
					trainEndPeriod = len(periods)
				}
				var trainPL periodList = periods[cp.TrainStartPeriod:trainEndPeriod]
				cl, guesses := cp.ClusteringMethod(trainPL, cp.ClusterNumber)
				writeClusteringResultFiles(path, c, cpj, cl, guesses)
			} else {
				fmt.Println("Read Clustering Model...")
				readClusteringResultFiles(path, c, cpj)
			}

			preProcess(cp)
			testStartPeriod := cp.TestStartPeriod
			if testStartPeriod > len(periods)-1 {
				testStartPeriod = len(periods) - 1
			}
			var pl periodList = periods[testStartPeriod:]
			pl.serve(cp)
			pl.postProcess()

			writeResultFile(path, c, cpj, pl)

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

func readRequestsFile(path string, config config) {
	f, err := os.Open(path + config.RequestsFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Println("Read Requests...")
	readRequests(f, config.PeriodDuration, config.RequestsColumn, config.RequestsComma)
}

func readClusteringResultFiles(path string, c config, cpj parametersJSON) {
	model := path + c.RequestsFileName +
		"_clustering_model_" + cpj.ClusteringMethod +
		"_" + strconv.Itoa(cpj.TrainStartPeriod) +
		"_" + strconv.Itoa(cpj.TrainEndPeriod) +
		"_" + strconv.Itoa(cpj.ClusterNumber) + ".json"
	f, err := os.Open(path + c.RequestsFileName +
		"_clustering_result_" + cpj.ClusteringMethod +
		"_" + strconv.Itoa(cpj.TrainStartPeriod) +
		"_" + strconv.Itoa(cpj.TrainEndPeriod) +
		"_" + strconv.Itoa(cpj.ClusterNumber) + ".csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	readClusteringResult(model, f)
}

func writeClusteringResultFiles(path string, c config, cpj parametersJSON, cl clientList, guesses []int) {
	clusteringModel.PersistToFile(path + c.RequestsFileName +
		"_clustering_model_" + cpj.ClusteringMethod +
		"_" + strconv.Itoa(cpj.TrainStartPeriod) +
		"_" + strconv.Itoa(cpj.TrainEndPeriod) +
		"_" + strconv.Itoa(cpj.ClusterNumber) + ".json")
	f, err := os.Create(path + c.RequestsFileName +
		"_clustering_result_" + cpj.ClusteringMethod +
		"_" + strconv.Itoa(cpj.TrainStartPeriod) +
		"_" + strconv.Itoa(cpj.TrainEndPeriod) +
		"_" + strconv.Itoa(cpj.ClusterNumber) + ".csv")
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

func writeResultFile(path string, c config, cpj parametersJSON, pl periodList) {
	if cpj.ResultFileName == "" {
		cpj.ResultFileName = c.RequestsFileName +
			"_result_" + cpj.SimilarityFormula +
			"_" + strconv.FormatBool(cpj.IsPeriodSimilarity) +
			"_" + strconv.Itoa(cpj.TrainStartPeriod) +
			"_" + strconv.Itoa(cpj.TrainEndPeriod) +
			"_" + strconv.Itoa(cpj.ClusterNumber) +
			"_" + strconv.FormatFloat(cpj.CooperationThreshold, 'f', 2, 64) +
			"_" + strconv.Itoa(cpj.FilesLimit) +
			"_" + strconv.Itoa(cpj.FileSize) +
			"_" + strconv.Itoa(cpj.CacheStorageSize) +
			"_" + strconv.Itoa(cpj.TestStartPeriod) +
			"_" + cpj.CachePolicy +
			"_" + strconv.FormatBool(cpj.IsAssignClustering) +
			"_" + strconv.FormatBool(cpj.IsOnlineLearning) +
			"_" + strconv.FormatFloat(cpj.LearningRate, 'f', 1, 64) +
			"_" + cpj.ClusteringMethod + ".csv"
	}
	f, err := os.Create(path + cpj.ResultFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.WriteString(fmt.Sprint(cpj) + "\n")

	f.WriteString("\nOverall Download Rate: " + strconv.FormatFloat(pl.calRate(), 'f', 5, 64) + "\n")

	f.WriteString("\nDownload Rate:\n")
	for _, p := range pl {
		f.WriteString(p.end.Format("2006-01-02 15") + "\t" + strconv.FormatFloat(p.dlRate, 'f', 5, 64) + "\n")
	}

	f.WriteString("\nFiles \\ Small Cells\n\t")
	for i, sc := range smallCells {
		f.WriteString("cell" + strconv.Itoa(sc.id+1))
		if i != len(smallCells)-1 {
			f.WriteString("\t")
		} else {
			f.WriteString("\n")
		}
	}
	for i, file := range filesList {
		f.WriteString("file" + strconv.Itoa(i+1) + "\t")
		for j, sc := range smallCells {
			pop := sc.popularitiesAccumulated[pl[len(pl)-1].id][file]
			pop -= sc.popularitiesAccumulated[pl[0].id][file]
			pop += sc.popularitiesPeriod[pl[0].id][file]
			f.WriteString(strconv.Itoa(pop))
			if j != len(smallCells)-1 {
				f.WriteString("\t")
			} else {
				f.WriteString("\n")
			}
		}
	}
	f.WriteString("\nusers\t")
	for i, sc := range smallCells {
		f.WriteString(strconv.Itoa(len(sc.clients)))
		if i != len(smallCells)-1 {
			f.WriteString("\t")
		} else {
			f.WriteString("\n")
		}
	}
	f.WriteString("new\t")
	for i, sc := range smallCells {
		f.WriteString(strconv.Itoa(newUserNum[sc.id]))
		if i != len(smallCells)-1 {
			f.WriteString("\t")
		} else {
			f.WriteString("\n")
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

	newUserNum = make([]int, len(smallCells))
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
				if cp.IsOnlineLearning {
					onlineLearn(cp.LearningRate, clientList{c})
				}
				c.assign(cp, filter)
				newUserNum[c.smallCell.id]++
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
	if cp.IsOnlineLearning {
		onlineLearn(cp.LearningRate, p.newClients)
	} else {
		for _, c := range p.newClients {
			c.removeFrom(c.smallCell)
		}
		for _, c := range p.newClients {
			c.assign(cp, filter)
		}
	}
	for _, c := range p.newClients {
		newUserNum[c.smallCell.id]++
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
	guess, err := clusteringModel.Predict(c.getFilePopularity(periods[:periodNo+1]))
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

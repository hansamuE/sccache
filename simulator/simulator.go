package simulator

import (
	"fmt"
	"os"
	"sort"
	"strconv"
)

var (
	formula       similarityFormula
	smallCellSize int
	cacheStorages cacheStorageList
	periodNo      int
	newUserNum    []int
	dlFreq        [][]int
	dlFreqAll     []int
	log           string
	coop          [][]int
	iter          int
	dlRateTotal   float64
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
			smallCellSize = cp.SmallCellSize
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

			coop = readCooperationResultFiles(path, cpj)

			testStartPeriod := cp.TestStartPeriod
			if testStartPeriod > len(periods)-1 {
				testStartPeriod = len(periods) - 1
			}
			var pl periodList = periods[testStartPeriod:]

			iter = 10
			dlRateTotal = 0
			for k := 0; k < iter; k++ {
				preProcess(cp)
				pl.serve(cp)
				pl.postProcess()
				dlRateTotal += pl.calRate()
			}

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

func readCooperationResultFiles(path string, cpj parametersJSON) [][]int {
	f, err := os.Open(path + "coop_" + cpj.ResultFileName)
	//f, err := os.Open(path + c.RequestsFileName +
	//	"_cooperation_result_" + cpj.ClusteringMethod +
	//	"_" + strconv.Itoa(cpj.TrainStartPeriod) +
	//	"_" + strconv.Itoa(cpj.TrainEndPeriod) +
	//	"_" + strconv.Itoa(cpj.ClusterNumber) + ".csv")
	if err != nil {
		return nil
	}
	defer f.Close()
	return readCooperationResult(f)
}

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
			"_" + strconv.Itoa(cpj.SmallCellSize) +
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

	f.WriteString("\nAverage Download Rate: " + strconv.FormatFloat(dlRateTotal/float64(iter), 'f', 5, 64) + "\n")

	f.WriteString("\nOverall Download Rate: " + strconv.FormatFloat(pl.calRate(), 'f', 5, 64) + "\n")

	f.WriteString("\nDownload Rate:\n")
	for _, p := range pl {
		f.WriteString(p.end.Format("2006-01-02 15:04") + "\t" + strconv.FormatFloat(p.dlRate, 'f', 5, 64) + "\n")
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
	f.WriteString("addict\t")
	for i, sc := range smallCells {
		count := 0
		for _, c := range sc.clients {
			if len(c.popularityAccumulated[pl[len(pl)-1].id]) >= int(float64(len(filesList))*0.8) {
				count++
			}
		}
		f.WriteString(strconv.Itoa(count))
		if i != len(smallCells)-1 {
			f.WriteString("\t")
		} else {
			f.WriteString("\n")
		}
	}
	f.WriteString("\nDL\t")
	for _, sc := range smallCells {
		f.WriteString(strconv.Itoa(len(dlFreq[sc.id])-1) + "\t")
	}
	f.WriteString(strconv.Itoa(len(dlFreqAll)-1) + "\n")

	//f.WriteString("\nDownload Frequency:\n")
	//for i := range dlFreq {
	//	f.WriteString(strconv.Itoa(i+1) + ": " + fmt.Sprintln(dlFreq[i]))
	//}
	//f.WriteString("all: " + fmt.Sprintln(dlFreqAll))

	f.WriteString("\nCooperation:\n")
	for _, cs := range cacheStorages {
		for _, sc := range cs.smallCells {
			f.WriteString(strconv.Itoa(sc.id) + "\t")
		}
		f.WriteString("\n")
	}

	f.WriteString(log)
}

func preProcess(cp parameters) {
	log = ""

	smallCells.arrangeCooperation(cp)
	for _, f := range files {
		f.size = cp.FileSize
	}
	for _, cs := range cacheStorages {
		cs.size = cp.SmallCellSize * len(cs.smallCells)
		cs.space = cs.size
	}

	newUserNum = make([]int, len(smallCells))
	dlFreq = make([][]int, len(smallCells))
	for i := range dlFreq {
		dlFreq[i] = make([]int, 1)
	}
	dlFreqAll = make([]int, 1)
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
		c.smallCell.periodStats[p.id].served += sizeCached
		c.smallCell.periodStats[p.id].downloaded += f.size - sizeCached

		if f.size-sizeCached > 0 {
			dlFreq[c.smallCell.id] = append(dlFreq[c.smallCell.id], 0)
			dlFreqAll = append(dlFreqAll, 0)
		} else {
			dlFreq[c.smallCell.id][len(dlFreq[c.smallCell.id])-1]++
			dlFreqAll[len(dlFreqAll)-1]++
		}
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

	total := make(map[*file]int, len(filesList))
	for _, f := range filesList {
		for _, sc := range smallCells {
			total[f] += sc.popularitiesAccumulated[p.id][f]
			//total[f] += sc.popularitiesPeriod[p.id][f]
		}
	}

	if cp.IsOnlineCooperation {
		isCoop := make([]bool, len(smallCells))
		for _, f := range filesList {
			coopList := make([]*smallCell, 0)
			for _, sc := range smallCells {
				if isCoop[sc.id] {
					continue
				}
				if float64(sc.popularitiesAccumulated[p.id][f]-sc.popularitiesAccumulated[cp.TrainStartPeriod][f]+sc.popularitiesPeriod[cp.TrainStartPeriod][f])/float64(total[f]) >= cp.OnlineCoopThreshold {
					//if float64(sc.popularitiesPeriod[p.id][f])/float64(total[f]) >= cp.OnlineCoopThreshold {
					coopList = append(coopList, sc)
				}
			}
			if len(coopList) > 1 {
				cs := coopList[0].cacheStorage
				for _, sc := range coopList {
					if ok, cf := sc.cacheStorage.hasFile(f); ok {
						if cf.fixed {
							cs = sc.cacheStorage
							break
						}
					}
				}
				for _, sc := range coopList {
					if sc.cacheStorage != cs {
						sc.assignTo(cs)
					}
				}
				sizeCached, cf := cs.cacheFile(f, cp.CachePolicy)
				cf.fixed = true
				//cf.fixed = false
				cs.served += sizeCached
				cs.downloaded += f.size - sizeCached
				p.served += sizeCached
				p.downloaded += f.size - sizeCached
				for _, coopSC := range coopList {
					isCoop[coopSC.id] = true
				}
			}
		}
	}

	fmt.Println("End Period:", p.end)
	log += p.getData()
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
	for _, sc := range smallCells {
		sc.periodStats = make([]stats, len(periods))
	}
}

func (p *period) getData() string {
	data := ""

	data += fmt.Sprint("\nEnd Period:", p.end)
	data += fmt.Sprint("\nFiles \\ Small Cells\n\t")
	for i, sc := range smallCells {
		data += fmt.Sprint("cell" + strconv.Itoa(sc.id))
		if i != len(smallCells)-1 {
			data += fmt.Sprint("\t")
		} else {
			data += fmt.Sprint("total\n")
		}
	}
	cellTotal := make([]int, len(smallCells))
	for i, file := range filesList {
		fileTotal := 0
		data += fmt.Sprint("file" + strconv.Itoa(i+1) + "\t")
		for j, sc := range smallCells {
			pop := sc.popularitiesPeriod[p.id][file]
			data += fmt.Sprint(strconv.Itoa(pop))
			fileTotal += pop
			cellTotal[j] += pop
			if j != len(smallCells)-1 {
				data += fmt.Sprint("\t")
			} else {
				data += fmt.Sprint("\t" + strconv.Itoa(fileTotal) + "\n")
			}
		}
	}
	data += fmt.Sprint("total")
	for i := range smallCells {
		data += fmt.Sprint("\t" + strconv.Itoa(cellTotal[i]))
	}
	data += fmt.Sprint("\n\nusers\t")
	for i, sc := range smallCells {
		data += fmt.Sprint(strconv.Itoa(len(sc.clients)))
		if i != len(smallCells)-1 {
			data += fmt.Sprint("\t")
		} else {
			data += fmt.Sprint("\n")
		}
	}
	data += fmt.Sprint("\nDL\t")
	for _, sc := range smallCells {
		data += fmt.Sprint(strconv.Itoa(sc.periodStats[p.id].downloaded) + "\t")
	}
	data += fmt.Sprint("\nCooperation:\n")
	for _, cs := range cacheStorages {
		data += fmt.Sprint("size" + strconv.Itoa(cs.size) + "\t")
		for _, c := range cs.caches {
			if c.fixed {
				data += fmt.Sprint(c.file.id + "\t")
			}
		}
		for _, sc := range cs.smallCells {
			data += fmt.Sprint(strconv.Itoa(sc.id) + "\t")
		}
		data += fmt.Sprint("\n")
	}
	data += fmt.Sprintln()

	return data
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

func (scl smallCellList) arrangeCooperation(cp parameters) cacheStorageList {
	group := make([]smallCellList, 0)
	if cp.CooperationThreshold < 0 {
		for _, sc := range scl {
			group = append(group, smallCellList{sc})
		}
	} else {
		if true {
			graph := make([][]int, len(scl))
			for i := range graph {
				graph[i] = make([]int, len(scl))
			}
			for _, p := range periods[cp.TrainStartPeriod : cp.TrainEndPeriod+1] {
				s := scl.calSimilarity(false, p.id, nil)
				for i := 0; i < len(scl); i++ {
					for j := i + 1; j < len(scl); j++ {
						if s[i][j] >= cp.CooperationThreshold {
							graph[i][j]++
							graph[j][i]++
						}
					}
				}

				log += fmt.Sprintln()
				for i := range s {
					for key, value := range s[i] {
						log += fmt.Sprint(strconv.FormatFloat(value, 'f', 2, 64))
						if key != len(s[i])-1 {
							log += fmt.Sprint("\t")
						} else {
							log += fmt.Sprint("\n")
						}
					}
				}
			}
			log += fmt.Sprintln()
			for i := range graph {
				for key, value := range graph[i] {
					log += fmt.Sprint(value)
					if key != len(graph[i])-1 {
						log += fmt.Sprint("\t")
					} else {
						log += fmt.Sprint("\n")
					}
				}
			}
			log += fmt.Sprintln()
			//} else {
			ok := make([]bool, len(scl))
			sim := scl.calSimilarity(true, periodNo, nil)

			log += fmt.Sprintln()
			for i := range sim {
				for key, value := range sim[i] {
					log += fmt.Sprint(strconv.FormatFloat(value, 'f', 2, 64))
					if key != len(sim[i])-1 {
						log += fmt.Sprint("\t")
					} else {
						log += fmt.Sprint("\n")
					}
				}
			}

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
					if sim[i][j] >= cp.CooperationThreshold {
						group[len(group)-1] = append(group[len(group)-1], scl[j])
						ok[j] = true
					}
				}
			}
		}
		if coop != nil {
			group = make([]smallCellList, 0)
			for _, g := range coop {
				group = append(group, make(smallCellList, 0))
				for _, sc := range g {
					group[len(group)-1] = append(group[len(group)-1], scl[sc])
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
	if sc.cacheStorage != nil {
		sc.removeFrom(sc.cacheStorage)
	}
	cs.smallCells = append(cs.smallCells, sc)
	cs.size += smallCellSize
	cs.space += smallCellSize
	sc.cacheStorage = cs

	for pn, fp := range sc.popularitiesAccumulated {
		if len(cs.popularitiesAccumulated)-1 < pn {
			cs.popularitiesAccumulated = append(cs.popularitiesAccumulated, make(popularities))
		}
		for k, v := range fp {
			cs.popularitiesAccumulated[pn][k] += v
		}
	}
}

func (sc *smallCell) removeFrom(cs *cacheStorage) {
	if len(cs.smallCells) == 1 {
		for i, cacheStorage := range cacheStorages {
			if cacheStorage == cs {
				cacheStorages = append(cacheStorages[:i], cacheStorages[i+1:]...)
				return
			}
		}
	}
	for i := range cs.smallCells {
		if cs.smallCells[i] == sc {
			cs.smallCells = append(cs.smallCells[:i], cs.smallCells[i+1:]...)
			break
		}
	}

	cs.size -= smallCellSize
	cs.space -= smallCellSize
	cs.caches = leastFrequentlyUsed(cs.caches)
	fixedSize := 0
	for _, c := range cs.caches {
		if c.fixed {
			fixedSize += c.size
		}
	}
	for _, c := range cs.caches {
		if fixedSize < cs.size {
			break
		}
		if c.fixed {
			c.fixed = false
			fixedSize -= c.size
		}
	}
	i := 0
	for cs.space < 0 {
		for cs.caches[i].fixed {
			i++
		}
		cs.space += cs.caches[i].size
		cs.deleteCache([]int{i})
	}

	for pn, fp := range sc.popularitiesAccumulated {
		for k, v := range fp {
			cs.popularitiesAccumulated[pn][k] -= v
		}
	}
}

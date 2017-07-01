package simulator

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/hansamuE/sccache/predictor"
)

var (
	trainStartPeriod int
	trainEndPeriod   int
	testStartPeriod  int
	formula          similarityFormula
	policy           cachePolicy
	fileSize         int
	smallCellSize    int
	cacheStorages    cacheStorageList
	periodNo         int
	newUserNum       []int
	dlFreq           [][]int
	dlFreqAll        []int
	log              string
	coop             [][]int
	iter             int
	dlRateTotal      float64
	predictors       predictorsList
	predictorC       []int
	predictorTotal   int
	mixedC           int
	mixedTotal       int
	reqThreshold     int
	cluThreshold     float64
	dlRateLog        string
)

type predictorsList []predictor.Predictors

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
		cj := configJSONs[i]

		readRequestsFile(path, c)

		trainStartPeriod = c.TrainStartPeriod
		if c.TrainDuration == -1 || trainEndPeriod > len(periods)-1 {
			trainEndPeriod = len(periods) - 1
		} else {
			trainEndPeriod = c.TrainStartPeriod + c.TrainDuration - 1
		}
		testStartPeriod = c.TestStartPeriod
		if testStartPeriod > len(periods)-1 {
			testStartPeriod = len(periods) - 1
		}
		formula = c.SimilarityFormula
		policy = c.CachePolicy
		fileSize = c.FileSize
		smallCellSize = c.SmallCellSize
		cluThreshold = c.ClusteringThreshold

		if !c.IsTrained {
			fmt.Println("Clustering...")
			var trainPL periodList = periods[trainStartPeriod : trainEndPeriod+1]
			cl, guesses := c.ClusteringMethod(trainPL, c.ClusterNumber)
			writeClusteringResultFiles(path, cj, cl, guesses)
		}

		coop = readCooperationResultFiles(path, c)

		var pl periodList = periods[testStartPeriod:]

		iter = c.SimIterations

		for _, cp := range configs[i].ParametersList {
			fmt.Println("Read Clustering Model...")
			readClusteringResultFiles(path, cj)

			dlRateTotal = 0
			for k := 0; k < iter; k++ {
				preProcess(cp)
				pl.serve(c, cp)
				pl.postProcess()
				dlRateTotal += pl.calRate()

				writeResultFile(path, cj, cp, pl)
				for _, p := range pl {
					p.downloaded = 0
					p.served = 0
				}
				for _, sc := range smallCells {
					sc.periodStats = make([]stats, len(periods))
				}
			}
			fmt.Println(dlRateTotal / float64(iter))

			reset()
		}
	}
	writeDownloadRateFile(path)
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

func readClusteringResultFiles(path string, cj configJSON) {
	model := path + cj.RequestsFileName +
		"_clustering_model_" + cj.ClusteringMethod +
		"_" + strconv.Itoa(cj.TrainStartPeriod) +
		"_" + strconv.Itoa(cj.TrainDuration) +
		"_" + strconv.Itoa(cj.ClusterNumber) + ".json"
	f, err := os.Open(path + cj.RequestsFileName +
		"_clustering_result_" + cj.ClusteringMethod +
		"_" + strconv.Itoa(cj.TrainStartPeriod) +
		"_" + strconv.Itoa(cj.TrainDuration) +
		"_" + strconv.Itoa(cj.ClusterNumber) + ".csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	readClusteringResult(model, f)
}

func writeClusteringResultFiles(path string, cj configJSON, cl clientList, guesses []int) {
	clusteringModel.PersistToFile(path + cj.RequestsFileName +
		"_clustering_model_" + cj.ClusteringMethod +
		"_" + strconv.Itoa(cj.TrainStartPeriod) +
		"_" + strconv.Itoa(cj.TrainDuration) +
		"_" + strconv.Itoa(cj.ClusterNumber) + ".json")
	f, err := os.Create(path + cj.RequestsFileName +
		"_clustering_result_" + cj.ClusteringMethod +
		"_" + strconv.Itoa(cj.TrainStartPeriod) +
		"_" + strconv.Itoa(cj.TrainDuration) +
		"_" + strconv.Itoa(cj.ClusterNumber) + ".csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for i, c := range cl {
		f.WriteString(c.id + "\t" + strconv.Itoa(guesses[i]) + "\n")
	}
}

func writeDownloadRateFile(path string) {
	f, err := os.Create(path + "download_rate.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	f.WriteString(dlRateLog)
}

//func readClustersFile(path string) {
//	f, err := os.Open(path + "clusters.csv")
//	if err != nil {
//		panic(err)
//	}
//	defer f.Close()
//	readClientsAssignment(f)
//}

func readCooperationResultFiles(path string, c config) [][]int {
	if c.CooperationFileName == "" {
		c.CooperationFileName = c.RequestsFileName + "_coop.csv"
	}
	f, err := os.Open(path + c.CooperationFileName)
	if err != nil {
		return nil
	}
	defer f.Close()
	return readCooperationResult(f)
}

func writeResultFile(path string, cj configJSON, cp parameters, pl periodList) {
	if cp.ResultFileName == "" {
		cp.ResultFileName = cj.RequestsFileName +
			"_" + strconv.FormatBool(cp.IsPeriodSimilarity) +
			"_" + strconv.Itoa(cj.TrainStartPeriod) +
			"_" + strconv.Itoa(cj.TrainDuration) +
			"_" + strconv.Itoa(cj.ClusterNumber) +
			"_" + strconv.FormatFloat(cp.CooperationThreshold, 'f', 2, 64) +
			"_" + strconv.Itoa(cp.FilesLimit) +
			"_" + strconv.Itoa(cj.FileSize) +
			"_" + strconv.Itoa(cj.SmallCellSize) +
			"_" + strconv.Itoa(cj.TestStartPeriod) +
			"_" + strconv.FormatBool(cp.IsAssignClustering) +
			"_" + strconv.FormatBool(cp.IsOnlineLearning) +
			"_" + strconv.FormatFloat(cp.LearningRate, 'f', 1, 64) +
			"_" + cj.ClusteringMethod + ".csv"
	}
	f, err := os.Create(path + cp.ResultFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.WriteString(fmt.Sprint(cp) + "\n")

	f.WriteString("\nAverage Download Rate: " + strconv.FormatFloat(dlRateTotal/float64(iter), 'f', 5, 64) + "\n")
	dlRateLog += fmt.Sprint(cp.ResultFileName + "\t" + strconv.FormatFloat(dlRateTotal/float64(iter), 'f', 5, 64) + "\n")

	f.WriteString("\nOverall Download Rate: " + strconv.FormatFloat(pl.calRate(), 'f', 5, 64) + "\n")

	f.WriteString("\nDownload Rate:\n")
	for _, p := range pl {
		f.WriteString(p.end.Format("2006-01-02 15:04") + "\t" + strconv.FormatFloat(p.dlRate, 'f', 5, 64) + "\n")
	}

	//f.WriteString("\nFiles \\ Small Cells\n\t")
	//for i, sc := range smallCells {
	//	f.WriteString("cell" + strconv.Itoa(sc.id))
	//	if i != len(smallCells)-1 {
	//		f.WriteString("\t")
	//	} else {
	//		f.WriteString("\n")
	//	}
	//}
	//for i, file := range filesList {
	//	f.WriteString("file" + strconv.Itoa(i+1) + "\t")
	//	for j, sc := range smallCells {
	//		pop := sc.popularitiesAccumulated[pl[len(pl)-1].id][file]
	//		pop -= sc.popularitiesAccumulated[pl[0].id][file]
	//		pop += sc.popularitiesPeriod[pl[0].id][file]
	//		f.WriteString(strconv.Itoa(pop))
	//		if j != len(smallCells)-1 {
	//			f.WriteString("\t")
	//		} else {
	//			f.WriteString("\n")
	//		}
	//	}
	//}
	//f.WriteString("\nusers\t")
	//for i, sc := range smallCells {
	//	f.WriteString(strconv.Itoa(len(sc.clients)))
	//	if i != len(smallCells)-1 {
	//		f.WriteString("\t")
	//	} else {
	//		f.WriteString("\n")
	//	}
	//}
	//f.WriteString("new\t")
	//for i, sc := range smallCells {
	//	f.WriteString(strconv.Itoa(newUserNum[sc.id]))
	//	if i != len(smallCells)-1 {
	//		f.WriteString("\t")
	//	} else {
	//		f.WriteString("\n")
	//	}
	//}
	//f.WriteString("addict\t")
	//for i, sc := range smallCells {
	//	count := 0
	//	for _, cj := range sc.clients {
	//		if len(cj.popularityAccumulated[pl[len(pl)-1].id]) >= int(float64(len(filesList))*0.8) {
	//			count++
	//		}
	//	}
	//	f.WriteString(strconv.Itoa(count))
	//	if i != len(smallCells)-1 {
	//		f.WriteString("\t")
	//	} else {
	//		f.WriteString("\n")
	//	}
	//}
	//f.WriteString("\nDL\t")
	//for _, sc := range smallCells {
	//	f.WriteString(strconv.Itoa(len(dlFreq[sc.id])-1) + "\t")
	//}
	//f.WriteString(strconv.Itoa(len(dlFreqAll)-1) + "\n")

	f.WriteString(pl[len(pl)-1].getData(false))

	//f.WriteString("\nDownload Frequency:\n")
	//for i := range dlFreq {
	//	f.WriteString(strconv.Itoa(i+1) + ": " + fmt.Sprintln(dlFreq[i]))
	//}
	//f.WriteString("all: " + fmt.Sprintln(dlFreqAll))

	//f.WriteString("\nCooperation:\n")
	//for _, cs := range cacheStorages {
	//	for _, sc := range cs.smallCells {
	//		f.WriteString(strconv.Itoa(sc.id) + "\t")
	//	}
	//	f.WriteString("\n")
	//}

	f.WriteString(log)
}

func preProcess(cp parameters) {
	log = ""

	smallCells.arrangeCooperation(cp)
	for _, f := range files {
		f.size = fileSize
	}
	for _, cs := range cacheStorages {
		cs.size = smallCellSize * len(cs.smallCells)
		cs.space = cs.size
	}

	newUserNum = make([]int, len(smallCells))
	dlFreq = make([][]int, len(smallCells))
	for i := range dlFreq {
		dlFreq[i] = make([]int, 1)
	}
	dlFreqAll = make([]int, 1)

	predictors = make(predictorsList, 0)
	predictors = append(predictors, predictor.NewDES("DES 0.99", 0.99))
	//predictors = append(predictors, predictor.NewCB("CB 1", 1))
	//predictors = append(predictors, predictor.NewDB("DB"))
	//predictors = append(predictors, predictor.NewAMA("AMA 3", 3))
	predictors = append(predictors, predictor.NewAMA("AMA 7", 7))
	//predictors = append(predictors, predictor.NewAMA("AMA 14", 14))
	//predictors = append(predictors, predictor.NewGMA("GMA 3", 3))
	predictors = append(predictors, predictor.NewGMA("GMA 7", 7))
	//predictors = append(predictors, predictor.NewGMA("GMA 14", 14))

	predictorC = make([]int, len(predictors))
	predictorTotal = 0
	mixedC = 0
	mixedTotal = 0
}

func (pl periodList) serve(c config, cp parameters) {
	log += periods[pl[0].id-1].getData(false)
	fmt.Println("Start Testing With Config:", cp)
	for pn, p := range pl {
		if cp.IsPredictive {
			cacheStorages.setPopularFiles(p.id)
			for _, cs := range cacheStorages {
				popFileQ := smallCellSize / fileSize * len(cs.smallCells)
				fixedFileQ := int(float64(smallCellSize/fileSize*len(cs.smallCells))*cp.ProportionFixed + 0.5)
				if cp.ProportionFixed != 0 && fixedFileQ == 0 {
					fixedFileQ = 1
				}
				//input := make([]popularities, 0)
				//for i := p.id%2; i+1 < p.id; i+=2 {
				//	input = append(input, make(popularities))
				//	for f, pop := range cs.popularitiesAccumulated[i+1] {
				//		input[len(input)-1][f] = pop
				//	}
				//}
				//fll := predictors.predictFileRankings(input)
				fll := predictors.predictFileRankings(cs.popularitiesAccumulated[:p.id])
				log += "\n\nReal:\n\t"
				for i := 0; i < popFileQ; i++ {
					log += "\t" + cs.popularFiles[p.id][i].name
				}
				fileCount := make(popularities, 0)
				for i, fl := range fll {
					log += "\n" + predictors[i].Name() + ":\n\t"
					for j := 0; j < popFileQ; j++ {
						log += "\t" + fl[j].name
						fileCount[fl[j]] += 2
						if j <= 1 {
							fileCount[fl[j]] += 2 - j
						}
					}
					n := len(cs.popularFiles[p.id][:popFileQ].intersect(fl[:popFileQ]))
					predictorC[i] += n
					predictorTotal += popFileQ
					log += "\t" + strconv.FormatFloat(float64(n)/float64(popFileQ), 'f', 2, 64)
				}
				fileCountList := make(filePopularityList, 0)
				for f, pop := range fileCount {
					fileCountList = append(fileCountList, filePopularity{f, pop})
				}
				sort.Stable(fileCountList)
				mixedFl := fileCountList.getFileList()

				if !cp.IsOfflinePredictive {
					for fileSize*(fixedFileQ+1) <= smallCellSize*len(cs.smallCells) && fileCountList[fixedFileQ].popularity == fileCountList[fixedFileQ-1].popularity {
						fixedFileQ++
					}
					//for fixedFileQ >= 1 && fileCountList[fixedFileQ - 1].popularity == 2 {
					//	fixedFileQ--
					//}
				}

				log += "\nMixed:\n\t"
				for i := 0; i < fixedFileQ; i++ {
					log += "\t" + mixedFl[i].name
				}
				n := len(cs.popularFiles[p.id][:popFileQ].intersect(mixedFl[:fixedFileQ]))
				mixedC += n
				mixedTotal += fixedFileQ
				log += "\t" + strconv.FormatFloat(float64(n)/float64(fixedFileQ), 'f', 2, 64)
				var popularFiles fileList
				if cp.IsOfflinePredictive {
					popularFiles = cs.popularFiles[p.id]
				} else {
					popularFiles = mixedFl
				}
				for _, cache := range cs.caches {
					cache.fixed = false
				}
				//di := make([]int, 0)
				//cs.caches = cp.CachePolicy(cs.caches)
				//for i, c := range cs.caches {
				//	isPopular := false
				//	for _, f := range popularFiles[:fixedFileQ] {
				//		if c.file == f {
				//			isPopular = true
				//			break
				//		}
				//	}
				//	if !isPopular {
				//		di = append(di, i)
				//	}
				//}
				//for _, v := range di {
				//	cs.space += cs.caches[v].size
				//}
				//cs.deleteCache(di)
				for _, f := range popularFiles[:fixedFileQ] {
					sizeCached, cf := cs.cacheFile(f, policy)
					cf.fixed = true
					cs.downloaded += f.size - sizeCached
					p.downloaded += f.size - sizeCached
				}
			}
			log += "\n"
		}

		filesLimit := cp.FilesLimit
		if filesLimit > len(p.popularFiles) {
			filesLimit = len(p.popularFiles)
		}
		p.serve(cp, p.popularFiles[:filesLimit])
		if cp.IsPeriodSimilarity {
			p.endPeriod(c, cp, pl[pn+1].popularFiles[:filesLimit])
		} else {
			p.endPeriod(c, cp, nil)
		}
	}
	fmt.Println("All Periods Tested")

	for i, total := range predictorC {
		log += "\n" + predictors[i].Name() + ":\n\t" + strconv.FormatFloat(float64(total)/float64(predictorTotal/len(predictors)), 'f', 2, 64)
	}
	log += "\nMixed:\n\t" + strconv.FormatFloat(float64(mixedC)/float64(mixedTotal), 'f', 2, 64)
}

func (p *period) serve(cp parameters, filter fileList) {
	periodNo = p.id
	//checkPoint := []int{int(len(periods[periodNo-1].requests)/3), int(len(periods[periodNo-1].requests)*2/3)}
	//c := 0
	checkDuration, err := time.ParseDuration(strconv.Itoa(int(p.end.Sub(periods[p.id-1].end).Nanoseconds()/3)) + "ns")
	if err != nil {
		panic(err)
	}
	checkTime := periods[p.id-1].end.Add(checkDuration)
	count := make(map[*cacheStorage]popularities)
	for _, cs := range cacheStorages {
		count[cs] = make(popularities)
	}
	for _, r := range p.requests {
		t, f, c := r.time, r.file, r.client
		if cp.IsPredictive && !cp.IsOfflinePredictive && t.After(checkTime) {
			checkTime = checkTime.Add(checkDuration)
			for _, cs := range cacheStorages {
				fpl := make(filePopularityList, 0)
				for file, pop := range count[cs] {
					fpl = append(fpl, filePopularity{file, pop})
				}
				sort.Stable(fpl)
				fl := fpl.getFileList()
				for _, cache := range cs.caches {
					if !cache.fixed {
						continue
					}
					isPop := false
					for _, popFile := range fl[:smallCellSize/fileSize*len(cs.smallCells)] {
						if cache.file == popFile {
							isPop = true
							break
						}
					}
					if !isPop {
						cache.fixed = false
					}
				}
				for _, popFile := range fl[:int(float64(smallCellSize/fileSize*len(cs.smallCells))*cp.ProportionFixed)] {
					//for _, popFile := range fl[:cp.SmallCellSize/cp.FileSize*len(cs.smallCells)-1] {
					sizeCached, cf := cs.cacheFile(popFile, policy)
					cf.fixed = true
					cs.downloaded += f.size - sizeCached
					p.downloaded += f.size - sizeCached
				}
			}
		}
		if len(filter) != 0 && !filter.has(f) {
			continue
		}
		if c.smallCell == nil {
			total := 0
			for _, pop := range c.popularityAccumulated[periodNo-1] {
				total += pop
			}
			if total < reqThreshold {
				cacheStorages.assignNewClient(c, f)
				p.newClients = append(p.newClients, c)
			} else {
				if cp.IsOnlineLearning {
					onlineLearn(cp.LearningRate, clientList{c})
				}
				c.assign(cp, periods[:p.id], periods[p.id-1].popularFiles[:len(smallCells)-2].unite(periods[p.id-2].popularFiles[:len(smallCells)-2]))
				//c.assign(cp, periods[:p.id], filter)
			}
			newUserNum[c.smallCell.id]++
		}

		cs := c.smallCell.cacheStorage
		sizeCached, cf := cs.cacheFile(f, policy)
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

		count[cs][f]++
	}
}

func (p *period) endPeriod(c config, cp parameters, filter fileList) {
	fmt.Println("End Period:", p.end)
	log += p.getData(true)

	p.calRate()
	if !c.IsAssignmentFixed {
		if cp.IsOnlineLearning {
			onlineLearn(cp.LearningRate, p.newClients)
		} else {
			for _, c := range p.newClients {
				newUserNum[c.smallCell.id]--
				c.removeFrom(c.smallCell)
			}
			for _, c := range p.newClients {
				total := 0
				for _, pop := range c.popularityAccumulated[periodNo] {
					total += pop
				}
				if total >= reqThreshold {
					c.assign(cp, periods[:p.id+1], p.popularFiles[:len(smallCells)-2].unite(periods[p.id-1].popularFiles[:len(smallCells)-2]))
					//c.assign(cp, periods[:p.id+1], filter)
				} else {
					c.assignTo(smallCells[len(smallCells)-1])
				}
				newUserNum[c.smallCell.id]++
			}
			for _, c := range smallCells[len(smallCells)-1].clients {
				total := 0
				for _, pop := range c.popularityAccumulated[periodNo] {
					total += pop
				}
				if total >= reqThreshold {
					c.assign(cp, periods[:p.id+1], p.popularFiles[:len(smallCells)-2].unite(periods[p.id-1].popularFiles[:len(smallCells)-2]))
					//c.assign(cp, periods[:p.id+1], filter)
					if c.smallCell.id != len(smallCells)-1 {
						newUserNum[c.smallCell.id]++
						newUserNum[len(smallCells)-1]--
					}
				}
			}
		}
	}
	//for _, c := range p.newClients {
	//	newUserNum[c.smallCell.id]++
	//}

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
				if float64(sc.popularitiesAccumulated[p.id][f]-sc.popularitiesAccumulated[trainStartPeriod][f]+sc.popularitiesPeriod[trainStartPeriod][f])/float64(total[f]) >= cp.OnlineCoopThreshold {
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
				sizeCached, cf := cs.cacheFile(f, policy)
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
	//for _, sc := range smallCells {
	//	sc.cacheStorage = nil
	//sc.periodStats = make([]stats, len(periods))
	//}
}

func (pl predictorsList) predictFileRankings(pops []popularities) []fileList {
	fll := make([]fileList, 0)
	fpll := make([]filePopularityList, len(pl))
	for _, f := range filesList {
		input := make([]int, 0)
		for _, pop := range pops {
			input = append(input, pop[f])
		}
		for i, p := range pl {
			if fpll[i] == nil {
				fpll[i] = make(filePopularityList, 0)
			}
			pop, err := p.Predict(input)
			if err != nil {
				panic(err)
			}
			fpll[i] = append(fpll[i], filePopularity{f, pop[len(input)] - input[len(input)-1]})
		}
	}
	for _, fpl := range fpll {
		sort.Stable(fpl)
		fll = append(fll, fpl.getFileList())
	}
	return fll
}

func (p *period) getData(isPeriod bool) string {
	data := ""

	data += fmt.Sprint("\nEnd Period:", p.end)

	data += "\n\nOverall:\n\t"
	for i := 0; i < len(smallCells)-2; i++ {
		if isPeriod {
			data += "\t" + p.popularFiles[i].name
		} else {
			data += "\t" + p.popularFilesAccumulated[i].name
		}
	}

	data += fmt.Sprint("\nFiles \\ Small Cells\n\t")
	for i, sc := range smallCells {
		data += fmt.Sprint("cell" + strconv.Itoa(sc.id) + "\t")
		if i == len(smallCells)-1 {
			data += fmt.Sprint("total\n")
		}
	}
	cellTotal := make([]int, len(smallCells))
	for _, file := range filesList {
		fileTotal := 0
		data += fmt.Sprint(file.name + "\t")
		for j, sc := range smallCells {
			var pop int
			if isPeriod {
				pop = sc.popularitiesPeriod[p.id][file]
			} else {
				// needs to start from actual train period
				pop = sc.popularitiesAccumulated[p.id][file]
			}
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
	total := 0
	for i := range smallCells {
		data += fmt.Sprint("\t" + strconv.Itoa(cellTotal[i]))
		total += cellTotal[i]
	}
	data += fmt.Sprint("\t" + strconv.Itoa(total))
	data += fmt.Sprint("\n\nusers\t")
	total = 0
	for i, sc := range smallCells {
		data += fmt.Sprint(strconv.Itoa(len(sc.clients)))
		total += len(sc.clients)
		if i != len(smallCells)-1 {
			data += fmt.Sprint("\t")
		} else {
			data += fmt.Sprint("\t" + strconv.Itoa(total) + "\n")
		}
	}
	data += fmt.Sprint("new\t")
	total = 0
	for i, sc := range smallCells {
		data += fmt.Sprint(strconv.Itoa(newUserNum[sc.id]))
		total += newUserNum[sc.id]
		if i != len(smallCells)-1 {
			data += fmt.Sprint("\t")
		} else {
			data += fmt.Sprint("\t" + strconv.Itoa(total) + "\n")
		}
	}
	data += fmt.Sprint("\nDL\t")
	total = 0
	for _, sc := range smallCells {
		dl := 0
		if isPeriod {
			dl = sc.periodStats[p.id].downloaded
		} else {
			for i := 0; i < p.id+1; i++ {
				dl += sc.periodStats[i].downloaded
			}
		}
		data += fmt.Sprint(strconv.Itoa(dl) + "\t")
		total += dl
	}
	data += fmt.Sprint(strconv.Itoa(total))
	data += fmt.Sprint("\n\nCooperation:\n")
	for _, cs := range cacheStorages {
		data += fmt.Sprint("size" + strconv.Itoa(cs.size) + "\t")
		for _, c := range cs.caches {
			if c.fixed {
				data += fmt.Sprint(c.file.name + "\t")
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

func (c *client) assign(cp parameters, pl periodList, filter fileList) {
	if cp.IsAssignClustering {
		c.assignWithClusteringModel(pl)
	} else {
		c.assignWithSimilarity(formula, filter)
	}
}

func (c *client) assignWithClusteringModel(pl periodList) {
	guess, err := clusteringModel.Predict(c.getFilePopularity(pl)[:len(smallCells)-2])
	if err != nil {
		panic("prediction error")
	}
	c.assignTo(smallCells[int(guess[0])])
}

func (c *client) assignWithSimilarity(fn similarityFormula, filter fileList) {
	sim := c.calSimilarityWithSmallCells(filter)
	//log += fmt.Sprintln(sim)
	mi, ms := len(smallCells)-1, 0.0
	for i, s := range sim {
		if s > ms {
			mi, ms = i, s
		}
	}
	c.assignTo(smallCells[mi])
	//if mi == -1 {
	//	c.assignTo(smallCells.leastClients())
	//} else {
	//	c.assignTo(cacheStorages[mi].smallCells.leastClients())
	//}
}

func (csl cacheStorageList) assignNewClient(c *client, f *file) {
	scl := csl.smallCellsHasFile(f)
	if len(scl) != 0 {
		c.assignTo(scl.leastClients())
	} else {
		c.assignTo(smallCells[len(smallCells)-1])
		//	c.assignTo(smallCells.leastClients())
	}
}

func (scl smallCellList) leastClients() *smallCell {
	sort.SliceStable(scl, func(i, j int) bool { return len(scl[i].clients) < len(scl[j].clients) })
	return scl[0]
}

func (scl smallCellList) arrangeCooperation(cp parameters) cacheStorageList {
	group := make([]smallCellList, 0)
	if cp.CooperationThreshold < 0 {
		for _, sc := range scl {
			group = append(group, smallCellList{sc})
		}
	} else {
		//if false {
		//	graph := make([][]int, len(scl))
		//	for i := range graph {
		//		graph[i] = make([]int, len(scl))
		//	}
		//	for _, p := range periods[cp.TrainStartPeriod : cp.TrainDuration+1] {
		//		s := scl.calSimilarity(false, p.id, nil)
		//		for i := 0; i < len(scl); i++ {
		//			for j := i + 1; j < len(scl); j++ {
		//				if s[i][j] >= cp.CooperationThreshold {
		//					graph[i][j]++
		//					graph[j][i]++
		//				}
		//			}
		//		}
		//
		//		log += fmt.Sprintln()
		//		for i := range s {
		//			for key, value := range s[i] {
		//				log += fmt.Sprint(strconv.FormatFloat(value, 'f', 2, 64))
		//				if key != len(s[i])-1 {
		//					log += fmt.Sprint("\t")
		//				} else {
		//					log += fmt.Sprint("\n")
		//				}
		//			}
		//		}
		//	}
		//	log += fmt.Sprintln()
		//	for i := range graph {
		//		for key, value := range graph[i] {
		//			log += fmt.Sprint(value)
		//			if key != len(graph[i])-1 {
		//				log += fmt.Sprint("\t")
		//			} else {
		//				log += fmt.Sprint("\n")
		//			}
		//		}
		//	}
		//	log += fmt.Sprintln()
		//	//} else {
		//	ok := make([]bool, len(scl))
		//	sim := scl.calSimilarity(true, periodNo, nil)
		//
		//	log += fmt.Sprintln()
		//	for i := range sim {
		//		for key, value := range sim[i] {
		//			log += fmt.Sprint(strconv.FormatFloat(value, 'f', 2, 64))
		//			if key != len(sim[i])-1 {
		//				log += fmt.Sprint("\t")
		//			} else {
		//				log += fmt.Sprint("\n")
		//			}
		//		}
		//	}
		//
		//	for i := 0; i < len(scl); i++ {
		//		if ok[i] {
		//			continue
		//		}
		//		group = append(group, smallCellList{scl[i]})
		//		ok[i] = true
		//		for j := i + 1; j < len(scl); j++ {
		//			if ok[j] {
		//				continue
		//			}
		//			if sim[i][j] >= cp.CooperationThreshold {
		//				group[len(group)-1] = append(group[len(group)-1], scl[j])
		//				ok[j] = true
		//			}
		//		}
		//	}
		//}
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
		cacheStorages[i] = &cacheStorage{
			smallCells:              make(smallCellList, 0),
			popularFiles:            make([]fileList, len(periods)),
			popularFilesAccumulated: make([]fileList, len(periods)),
		}
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
			cs.popularitiesPeriod = append(cs.popularitiesPeriod, make(popularities))
		}
		for k, v := range fp {
			cs.popularitiesAccumulated[pn][k] += v
			if pv, ok := sc.popularitiesPeriod[pn][k]; ok {
				cs.popularitiesPeriod[pn][k] += pv
			}
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
			if pv, ok := sc.popularitiesPeriod[pn][k]; ok {
				cs.popularitiesPeriod[pn][k] -= pv
			}
		}
	}
}

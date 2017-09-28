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
	trainDuration    int
	trainEndPeriod   int
	testStartPeriod  int
	formula          similarityFormula
	policy           cachePolicy
	coopFileName     string
	fileSize         int
	filesLimit       int
	clusteringMethod func(periodList, int) (clientList, []int)
	clusterNumber    int
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

func Simulate(path, configName string) {
	readConfigsFile(path, configName)

	for i, c := range configs {
		cj := configJSONs[i]

		readRequestsFile(path, c)

		formula = c.SimilarityFormula
		policy = c.CachePolicy
		cluThreshold = c.ClusteringThreshold
		fileSize = c.FileSize
		clusteringMethod = c.ClusteringMethod
		clusterNumber = c.ClusterNumber

		trainStartPeriod = c.TrainStartPeriod
		trainDuration = c.TrainDuration
		trainEndPeriod = trainStartPeriod + trainDuration - 1
		if trainDuration == -1 || trainEndPeriod > len(periods)-1 {
			trainEndPeriod = len(periods) - 1
		}
		testStartPeriod = c.TestStartPeriod
		if testStartPeriod > len(periods)-1 {
			testStartPeriod = len(periods) - 1
		}

		if !c.IsTrained {
			fmt.Println("Clustering...")
			var trainPL periodList = periods[trainStartPeriod : trainEndPeriod+1]
			cl, guesses := clusteringMethod(trainPL, clusterNumber)
			writeClusteringResultFiles(path, cj, cl, guesses)
		}

		var pl periodList = periods[testStartPeriod:]

		iter = c.SimIterations

		cfnl := len(c.CooperationFileName)
		if cfnl == 0 {
			cfnl = 1
		}
		fll := len(c.FilesLimit)
		if fll == 0 {
			fll = 1
		}
		scsl := len(c.SmallCellSize)

		for fli := 0; fli < fll; fli++ {
			for cfni := 0; cfni < cfnl; cfni++ {
				for scsi := 0; scsi < scsl; scsi++ {
					for _, cp := range configs[i].ParametersList {
						fmt.Println("Read Clustering Model...")
						readClusteringResultFiles(path, cj)

						if len(c.FilesLimit) != 0 {
							filesLimit = c.FilesLimit[fli]
						} else {
							filesLimit = cp.FilesLimit
						}
						if len(c.CooperationFileName) != 0 {
							coopFileName = c.CooperationFileName[cfni]
						} else {
							coopFileName = ""
						}
						coop = readCooperationResultFiles(path, c.CooperationFileName[cfni], c)
						smallCellSize = c.SmallCellSize[scsi]

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
					dlRateLog += "\n"
				}
				dlRateLog += "\n"
			}
			dlRateLog += "\n"
		}
		dlRateLog += "\n"
	}
	writeDownloadRateFile(path, configName)
}

func readConfigsFile(path, configName string) {
	f, err := os.Open(path + configName)
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
	model := path + "model/" + cj.RequestsFileName +
		"_clustering_model_" + cj.ClusteringMethod +
		"_" + strconv.Itoa(cj.TrainStartPeriod) +
		"_" + strconv.Itoa(cj.TrainDuration) +
		"_" + strconv.Itoa(cj.ClusterNumber) + ".json"
	f, err := os.Open(path + "model/" + cj.RequestsFileName +
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
	err := os.MkdirAll(path+"model", os.ModePerm)
	if err != nil {
		panic(err)
	}
	clusteringModel.PersistToFile(path + "model/" + cj.RequestsFileName +
		"_clustering_model_" + cj.ClusteringMethod +
		"_" + strconv.Itoa(cj.TrainStartPeriod) +
		"_" + strconv.Itoa(cj.TrainDuration) +
		"_" + strconv.Itoa(cj.ClusterNumber) + ".json")
	f, err := os.Create(path + "model/" + cj.RequestsFileName +
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

func writeDownloadRateFile(path, configName string) {
	f, err := os.Create(path + "download_rate_" + configName + ".csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	f.WriteString(dlRateLog)
}

func readCooperationResultFiles(path string, fileName string, c config) [][]int {
	if fileName == "" {
		fileName = c.RequestsFileName + "_coop.csv"
	}
	f, err := os.Open(path + fileName)
	if err != nil {
		return nil
	}
	defer f.Close()
	return readCooperationResult(f)
}

func writeResultFile(path string, cj configJSON, cp parameters, pl periodList) {
	if cp.ResultFileName == "" {
		cp.ResultFileName = cj.RequestsFileName + cj.FileNamePreceded +
			"_" + strconv.FormatBool(cp.IsPeriodSimilarity) +
			"_" + strconv.Itoa(cj.TrainStartPeriod) +
			"_" + strconv.Itoa(cj.TrainDuration) +
			"_" + strconv.Itoa(cj.ClusterNumber) +
			"_" + strconv.FormatFloat(cp.CooperationThreshold, 'f', 2, 64) +
			"_" + strconv.Itoa(filesLimit) +
			"_" + strconv.Itoa(cj.FileSize) +
			"_" + strconv.Itoa(smallCellSize) +
			"_" + strconv.Itoa(cj.TestStartPeriod) +
			"_" + strconv.FormatBool(cp.IsAssignClustering) +
			"_" + strconv.FormatBool(cp.IsOnlineLearning) +
			"_" + strconv.FormatFloat(cp.LearningRate, 'f', 1, 64) +
			"_" + cj.ClusteringMethod + ".csv"
	} else {
		cp.ResultFileName = cj.FileNamePreceded + strconv.Itoa(filesLimit) + "_" + coopFileName + "_cache" + strconv.Itoa(smallCellSize) + "_" + cp.ResultFileName
	}
	err := os.MkdirAll(path+"result", os.ModePerm)
	if err != nil {
		panic(err)
	}
	f, err := os.Create(path + "result/" + cp.ResultFileName)
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

	f.WriteString(pl[len(pl)-1].getData(false))

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
	log += periods[trainEndPeriod].getData(false)
	fmt.Println("Start Testing With Config:", cp)
	for pn, p := range pl {
		fl := filesLimit
		if fl > len(periods[p.id-1].popularFilesAccumulated) {
			fl = len(periods[p.id-1].popularFilesAccumulated)
		}
		filter := periods[p.id-1].popularFilesAccumulated[:fl]

		if cp.IsPredictive {
			cacheStorages.setPopularFiles(p.id)
			for _, cs := range cacheStorages {
				popFileQ := smallCellSize / fileSize * len(cs.smallCells)
				fixedFileQ := int(float64(smallCellSize/fileSize*len(cs.smallCells))*cp.ProportionFixed + 0.5)
				if cp.ProportionFixed != 0 && fixedFileQ == 0 {
					fixedFileQ = 1
				}
				fll := predictors.predictFileRankings(cs.popularitiesAccumulated[:p.id], filter)
				pq := popFileQ
				if pq > len(cs.popularFiles[p.id].intersect(filter)) {
					pq = len(cs.popularFiles[p.id].intersect(filter))
				}
				log += "\n\nReal:\n\t"
				for i := 0; i < pq; i++ {
					log += "\t" + cs.popularFiles[p.id].intersect(filter)[i].name
				}
				fileCount := make(popularities, 0)
				for i, fl := range fll {
					fq := popFileQ
					if fq > len(fl) {
						fq = len(fl)
					}
					log += "\n" + predictors[i].Name() + ":\n\t"
					for j := 0; j < fq; j++ {
						log += "\t" + fl[j].name
						fileCount[fl[j]] += 2
						if j <= 1 {
							fileCount[fl[j]] += 2 - j
						}
					}
					n := len(cs.popularFiles[p.id].intersect(filter)[:pq].intersect(fl[:fq]))
					predictorC[i] += n
					predictorTotal += fq
					log += "\t" + strconv.FormatFloat(float64(n)/float64(fq), 'f', 2, 64)
				}
				fileCountList := make(filePopularityList, 0)
				for f, pop := range fileCount {
					fileCountList = append(fileCountList, filePopularity{f, pop})
				}
				sort.Stable(fileCountList)
				mixedFl := fileCountList.getFileList()

				if fixedFileQ > len(fileCountList)-1 {
					fixedFileQ = len(fileCountList) - 1
				}
				if !cp.IsOfflinePredictive {
					for fileSize*(fixedFileQ+1) <= smallCellSize*len(cs.smallCells) && fileCountList[fixedFileQ].popularity == fileCountList[fixedFileQ-1].popularity && fixedFileQ+1 < len(fileCountList) {
						fixedFileQ++
					}
				}

				log += "\nMixed:\n\t"
				for i := 0; i < fixedFileQ; i++ {
					log += "\t" + mixedFl[i].name
				}
				n := len(cs.popularFiles[p.id].intersect(filter)[:pq].intersect(mixedFl[:fixedFileQ]))
				mixedC += n
				mixedTotal += fixedFileQ
				log += "\t" + strconv.FormatFloat(float64(n)/float64(fixedFileQ), 'f', 2, 64)
				var popularFiles fileList
				if cp.IsOfflinePredictive {
					popularFiles = cs.popularFiles[p.id].intersect(filter)
				} else {
					popularFiles = mixedFl
				}
				for _, cache := range cs.caches {
					cache.fixed = false
				}
				q := fixedFileQ
				if q > len(popularFiles) {
					q = len(popularFiles)
				}
				for _, f := range popularFiles[:q] {
					sizeCached, cf := cs.cacheFile(f, policy)
					cf.fixed = true
					cs.downloaded += f.size - sizeCached
					p.downloaded += f.size - sizeCached
				}
			}
			log += "\n"
		}

		p.serve(cp, filter)
		if cp.IsPeriodSimilarity {
			p.endPeriod(c, cp, pl[pn+1].popularFiles[:fl])
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
				popFileQ := smallCellSize / fileSize * len(cs.smallCells)
				if popFileQ > len(fl) {
					popFileQ = len(fl)
				}
				for _, cache := range cs.caches {
					isPop := false
					for _, popFile := range fl[:popFileQ] {
						if cache.file == popFile {
							isPop = true
							break
						}
					}
					cache.fixed = isPop
				}
				for _, popFile := range fl[:int(float64(popFileQ)*cp.ProportionFixed)] {
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
					if c.smallCell.id != len(smallCells)-1 {
						newUserNum[c.smallCell.id]++
						newUserNum[len(smallCells)-1]--
					}
				}
			}
		}
	}

	total := make(map[*file]int, len(filesList))
	for _, f := range filesList {
		for _, sc := range smallCells {
			total[f] += sc.popularitiesAccumulated[p.id][f]
		}
	}

	if cp.IsOnlineCooperative {
		isCoop := make([]bool, len(smallCells))
		for _, f := range filesList {
			coopList := make([]*smallCell, 0)
			for _, sc := range smallCells {
				if isCoop[sc.id] {
					continue
				}
				if float64(sc.popularitiesAccumulated[p.id][f]-sc.popularitiesAccumulated[trainStartPeriod][f]+sc.popularitiesPeriod[trainStartPeriod][f])/float64(total[f]) >= cp.OnlineCoopThreshold {
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

	if c.IsReClustering && (p.id-trainEndPeriod)%trainDuration == 0 {
		fmt.Println("Re-clustering...")
		for _, sc := range smallCells {
			for _, c := range sc.clients {
				newUserNum[c.smallCell.id]--
				c.removeFrom(c.smallCell)
			}
		}
		endPeriod := p.id + trainDuration
		if endPeriod >= len(periods) {
			endPeriod = len(periods) - 1
		}
		_, _ = clusteringMethod(periods[p.id:endPeriod+1], clusterNumber)
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
	smallCells = nil
}

func (pl predictorsList) predictFileRankings(pops []popularities, filter fileList) []fileList {
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
		fll = append(fll, fpl.getFileList().intersect(filter))
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
	var fList fileList
	if isPeriod {
		fl := filesLimit
		if fl > len(periods[p.id-1].popularFilesAccumulated) {
			fl = len(periods[p.id-1].popularFilesAccumulated)
		}
		fList = periods[p.id-1].popularFilesAccumulated[:fl]
	} else {
		fList = filesList
	}
	for _, file := range fList {
		fileTotal := 0
		data += fmt.Sprint(file.name + "\t")
		for j, sc := range smallCells {
			var pop int
			if isPeriod {
				pop = sc.popularitiesPeriod[p.id][file]
			} else {
				// needs modification to calculate from actual train period if not start from the first period
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
		emptySC := 0
		for _, sc := range smallCells {
			if len(sc.clients) == 0 {
				emptySC++
			}
		}
		if float64(emptySC) < float64(clusterNumber)*0.6 {
			c.assignWithClusteringModel(pl)
		} else {
			c.assignWithSimilarity(formula, filter)
		}
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
	mi, ms := len(smallCells)-1, 0.0
	for i, s := range sim {
		if s > ms {
			mi, ms = i, s
		}
	}
	c.assignTo(smallCells[mi])
}

func (csl cacheStorageList) assignNewClient(c *client, f *file) {
	scl := csl.smallCellsHasFile(f)
	if len(scl) != 0 {
		c.assignTo(scl.leastClients())
	} else {
		c.assignTo(smallCells[len(smallCells)-1])
	}
}

func (scl smallCellList) leastClients() *smallCell {
	sort.SliceStable(scl, func(i, j int) bool { return len(scl[i].clients) < len(scl[j].clients) })
	return scl[0]
}

func (scl smallCellList) arrangeCooperation(cp parameters) cacheStorageList {
	group := make([]smallCellList, 0)
	if !cp.IsCooperative {
		for _, sc := range scl {
			group = append(group, smallCellList{sc})
		}
	} else {
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
			popularitiesPeriod:      make([]popularities, len(periods)),
			popularitiesAccumulated: make([]popularities, len(periods)),
			popularFiles:            make([]fileList, len(periods)),
			popularFilesAccumulated: make([]fileList, len(periods)),
		}
		for p := 0; p < len(periods); p++ {
			cacheStorages[i].popularitiesPeriod[p] = make(popularities)
			cacheStorages[i].popularitiesAccumulated[p] = make(popularities)
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

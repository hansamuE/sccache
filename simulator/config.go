package simulator

import "time"

type configList []config
type configJSONList []configJSON
type parametersList []parameters
type parametersListJSON []parametersJSON

type config struct {
	RequestsFileName string
	PeriodDuration   time.Duration
	RequestsColumn   []int
	RequestsComma    rune
	ParametersList   parametersList
}

type parameters struct {
	IsTrained            bool
	SimilarityFormula    similarityFormula
	IsPeriodSimilarity   bool
	TrainStartPeriod     int
	TrainEndPeriod       int
	ClusterNumber        int
	CooperationThreshold float64
	FilesLimit           int
	FileSize             int
	SmallCellSize        int
	TestStartPeriod      int
	CachePolicy          cachePolicy
	IsOnlineCooperation  bool
	OnlineCoopThreshold  float64
	IsAssignClustering   bool
	IsOnlineLearning     bool
	LearningRate         float64
	ClusteringMethod     func(periodList, int) (clientList, []int)
	ResultFileName       string
}

type configJSON struct {
	RequestsFileName   string             `json:"requests_file_name"`
	PeriodDuration     string             `json:"period_duration"`
	RequestsColumn     []int              `json:"requests_column"`
	RequestsComma      string             `json:"requests_comma"`
	ParametersListJSON parametersListJSON `json:"parameters_list"`
}

type parametersJSON struct {
	IsTrained            bool    `json:"is_trained"`
	SimilarityFormula    string  `json:"similarity_formula"`
	IsPeriodSimilarity   bool    `json:"is_period_similarity"`
	TrainStartPeriod     int     `json:"train_start_period"`
	TrainEndPeriod       int     `json:"train_end_period"`
	ClusterNumber        int     `json:"cluster_number"`
	CooperationThreshold float64 `json:"cooperation_threshold"`
	FilesLimit           int     `json:"files_limit"`
	FileSize             int     `json:"file_size"`
	SmallCellSize        int     `json:"small_cell_size"`
	TestStartPeriod      int     `json:"test_start_period"`
	CachePolicy          string  `json:"cache_policy"`
	IsOnlineCooperation  bool    `json:"is_online_cooperation"`
	OnlineCoopThreshold  float64 `json:"online_coop_threshold"`
	IsAssignClustering   bool    `json:"is_assign_clustering"`
	IsOnlineLearning     bool    `json:"is_online_learning"`
	LearningRate         float64 `json:"learning_rate"`
	ClusteringMethod     string  `json:"clustering_method"`
	ResultFileName       string  `json:"result_file_name"`
}

func (cjl configJSONList) toConfig() configList {
	var err error
	cl := make(configList, len(cjl))
	for i, cj := range cjl {
		c := &cl[i]
		c.ParametersList = make(parametersList, len(cj.ParametersListJSON))

		if cj.RequestsFileName == "" {
			c.RequestsFileName = "requests.csv"
		} else {
			c.RequestsFileName = cj.RequestsFileName
		}

		c.PeriodDuration, err = time.ParseDuration(cj.PeriodDuration)
		if err != nil {
			panic(err)
		}

		c.RequestsColumn = make([]int, len(cj.RequestsColumn))
		copy(c.RequestsColumn, cj.RequestsColumn)

		if cj.RequestsComma == "" {
			c.RequestsComma = '\t'
		} else {
			c.RequestsComma = []rune(cj.RequestsComma)[0]
		}

		for j, cpj := range cj.ParametersListJSON {
			cp := &c.ParametersList[j]

			switch cpj.SimilarityFormula {
			case "exponential":
				cp.SimilarityFormula = exponential
			case "cosine":
				cp.SimilarityFormula = cosine
			default:
				cp.SimilarityFormula = exponential
			}

			cp.IsPeriodSimilarity = cpj.IsPeriodSimilarity
			cp.IsTrained = cpj.IsTrained
			cp.TrainStartPeriod = cpj.TrainStartPeriod
			cp.TrainEndPeriod = cpj.TrainEndPeriod
			cp.ClusterNumber = cpj.ClusterNumber
			cp.CooperationThreshold = cpj.CooperationThreshold
			cp.FilesLimit = cpj.FilesLimit
			cp.FileSize = cpj.FileSize
			cp.SmallCellSize = cpj.SmallCellSize
			cp.TestStartPeriod = cpj.TestStartPeriod

			switch cpj.CachePolicy {
			case "leastRecentlyUsed":
				cp.CachePolicy = leastRecentlyUsed
			case "leastFrequentlyUsed":
				cp.CachePolicy = leastFrequentlyUsed
			default:
				cp.CachePolicy = leastFrequentlyUsed
			}

			cp.IsOnlineCooperation = cpj.IsOnlineCooperation
			cp.OnlineCoopThreshold = cpj.OnlineCoopThreshold
			cp.IsAssignClustering = cpj.IsAssignClustering
			cp.IsOnlineLearning = cpj.IsOnlineLearning
			cp.LearningRate = cpj.LearningRate

			if cpj.ClusteringMethod == "similarity" {
				cp.ClusteringMethod = clusteringWithSimilarity
				cp.IsAssignClustering = false
				cp.IsOnlineLearning = false
			} else {
				cp.ClusteringMethod = clustering
			}

			cp.ResultFileName = cpj.ResultFileName
		}
	}
	return cl
}

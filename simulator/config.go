package simulator

import "time"

type configList []config
type configJSONList []configJSON
type parametersList []parameters
type parametersListJSON []parametersJSON

type config struct {
	RequestsFileName    string
	PeriodDuration      time.Duration
	RequestsColumn      []int
	RequestsComma       rune
	IsTrained           bool
	TrainStartPeriod    int
	TrainDuration       int
	TestStartPeriod     int
	SimIterations       int
	SimilarityFormula   similarityFormula
	ClusterNumber       int
	ClusteringMethod    func(periodList, int) (clientList, []int)
	ClusteringThreshold float64
	CooperationFileName string
	FileSize            int
	SmallCellSize       int
	CachePolicy         cachePolicy
	IsAssignmentFixed   bool
	FileNamePreceded    string
	ParametersList      parametersList
}

type parameters struct {
	FilesLimit           int
	IsPeriodSimilarity   bool
	IsPredictive         bool
	IsOfflinePredictive  bool
	ProportionFixed      float64
	CooperationThreshold float64
	IsOnlineCooperation  bool
	OnlineCoopThreshold  float64
	IsAssignClustering   bool
	IsOnlineLearning     bool
	LearningRate         float64
	ResultFileName       string
}

type configJSON struct {
	RequestsFileName    string             `json:"requests_file_name"`
	PeriodDuration      string             `json:"period_duration"`
	RequestsColumn      []int              `json:"requests_column"`
	RequestsComma       string             `json:"requests_comma"`
	IsTrained           bool               `json:"is_trained"`
	TrainStartPeriod    int                `json:"train_start_period"`
	TrainDuration       int                `json:"train_duration"`
	TestStartPeriod     int                `json:"test_start_period"`
	SimIterations       int                `json:"sim_iterations"`
	SimilarityFormula   string             `json:"similarity_formula"`
	ClusterNumber       int                `json:"cluster_number"`
	ClusteringMethod    string             `json:"clustering_method"`
	ClusteringThreshold float64            `json:"clustering_threshold"`
	CooperationFileName string             `json:"cooperation_file_name"`
	FileSize            int                `json:"file_size"`
	SmallCellSize       int                `json:"small_cell_size"`
	CachePolicy         string             `json:"cache_policy"`
	IsAssignmentFixed   bool               `json:"is_assignment_fixed"`
	FileNamePreceded    string             `json:"file_name_preceded"`
	ParametersListJSON  parametersListJSON `json:"parameters_list"`
}

type parametersJSON struct {
	FilesLimit           int     `json:"files_limit"`
	IsPeriodSimilarity   bool    `json:"is_period_similarity"`
	IsPredictive         bool    `json:"is_predictive"`
	IsOfflinePredictive  bool    `json:"is_offline_predictive"`
	ProportionFixed      float64 `json:"proportion_fixed"`
	CooperationThreshold float64 `json:"cooperation_threshold"`
	IsOnlineCooperation  bool    `json:"is_online_cooperation"`
	OnlineCoopThreshold  float64 `json:"online_coop_threshold"`
	IsAssignClustering   bool    `json:"is_assign_clustering"`
	IsOnlineLearning     bool    `json:"is_online_learning"`
	LearningRate         float64 `json:"learning_rate"`
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

		c.IsTrained = cj.IsTrained
		c.TrainStartPeriod = cj.TrainStartPeriod
		c.TrainDuration = cj.TrainDuration
		c.TestStartPeriod = cj.TestStartPeriod

		if cj.SimIterations == 0 {
			c.SimIterations = 1
		} else {
			c.SimIterations = cj.SimIterations
		}

		switch cj.SimilarityFormula {
		case "exp":
			c.SimilarityFormula = exponential
		case "cos":
			c.SimilarityFormula = cosine
		default:
			c.SimilarityFormula = exponential
		}

		c.ClusterNumber = cj.ClusterNumber

		if cj.ClusteringMethod == "similarity" {
			c.ClusteringMethod = clusteringWithSimilarity
		} else {
			c.ClusteringMethod = clustering
		}

		if cj.ClusteringThreshold == 0 {
			c.ClusteringThreshold = 0.5
		} else {
			c.ClusteringThreshold = cj.ClusteringThreshold
		}

		c.CooperationFileName = cj.CooperationFileName
		c.FileSize = cj.FileSize
		c.SmallCellSize = cj.SmallCellSize

		switch cj.CachePolicy {
		case "LRU":
			c.CachePolicy = leastRecentlyUsed
		case "LFU":
			c.CachePolicy = leastFrequentlyUsed
		default:
			c.CachePolicy = leastFrequentlyUsed
		}

		c.IsAssignmentFixed = cj.IsAssignmentFixed

		c.FileNamePreceded = cj.FileNamePreceded

		for j, cpj := range cj.ParametersListJSON {
			cp := &c.ParametersList[j]

			cp.FilesLimit = cpj.FilesLimit
			cp.IsPeriodSimilarity = cpj.IsPeriodSimilarity

			cp.IsPredictive = cpj.IsPredictive
			cp.IsOfflinePredictive = cpj.IsOfflinePredictive
			cp.ProportionFixed = cpj.ProportionFixed

			cp.CooperationThreshold = cpj.CooperationThreshold
			cp.IsOnlineCooperation = cpj.IsOnlineCooperation
			cp.OnlineCoopThreshold = cpj.OnlineCoopThreshold

			cp.IsAssignClustering = cpj.IsAssignClustering
			cp.IsOnlineLearning = cpj.IsOnlineLearning
			cp.LearningRate = cpj.LearningRate
			cp.ResultFileName = cj.FileNamePreceded + cpj.ResultFileName
		}
	}
	return cl
}

package simulator

import "time"

type configList []config
type configJSONList []configJSON
type parametersList []parameters
type parametersListJSON []parametersJSON

type config struct {
	PeriodDuration time.Duration
	RequestsColumn []int
	RequestsComma  string
	ParametersList parametersList
}

type parameters struct {
	IsTrained            bool
	SimilarityFormula    similarityFormula
	IsPeriodSimilarity   bool
	TrainStartPeriod     int
	TrainEndPeriod       int
	ClusterNumber        int
	CooperationThreshold float64
	TestStartPeriod      int
	CachePolicy          cachePolicy
	IsAssignClustering   bool
	IsOnlineLearning	bool
	ClusteringMethod     func(periodList, int) (clientList, []int)
	FilesLimit           int
	FileSize             int
	CacheStorageSize     int
	ResultFileName       string
}

type configJSON struct {
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
	TestStartPeriod      int     `json:"test_start_period"`
	CachePolicy          string  `json:"cache_policy"`
	IsAssignClustering   bool    `json:"is_assign_clustering"`
	IsOnlineLearning	bool        `json:"is_online_learning"`
	ClusteringMethod     string  `json:"clustering_method"`
	FilesLimit           int     `json:"files_limit"`
	FileSize             int     `json:"file_size"`
	CacheStorageSize     int     `json:"cache_storage_size"`
	ResultFileName       string  `json:"result_file_name"`
}

func (cjl configJSONList) toConfig() configList {
	var err error
	cl := make(configList, len(cjl))
	for i, cj := range cjl {
		c := &cl[i]
		c.ParametersList = make(parametersList, len(cj.ParametersListJSON))
		c.PeriodDuration, err = time.ParseDuration(cj.PeriodDuration)
		if err != nil {
			panic(err)
		}
		c.RequestsColumn = make([]int, len(cj.RequestsColumn))
		copy(c.RequestsColumn, cj.RequestsColumn)
		c.RequestsComma = cj.RequestsComma
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
			cp.TrainStartPeriod = cpj.TrainEndPeriod
			cp.ClusterNumber = cpj.ClusterNumber
			cp.CooperationThreshold = cpj.CooperationThreshold
			cp.TestStartPeriod = cpj.TestStartPeriod

			switch cpj.CachePolicy {
			case "leastRecentlyUsed":
				cp.CachePolicy = leastRecentlyUsed
			case "leastFrequentlyUsed":
				cp.CachePolicy = leastFrequentlyUsed
			default:
				cp.CachePolicy = leastFrequentlyUsed
			}

			cp.IsAssignClustering = cpj.IsAssignClustering
			cp.IsOnlineLearning = cpj.IsOnlineLearning

			if cpj.ClusteringMethod == "similarity" {
				cp.ClusteringMethod = clusteringWithSimilarity
				cp.IsAssignClustering = false
				cp.IsOnlineLearning = false
			} else {
				cp.ClusteringMethod = clustering
			}

			cp.FilesLimit = cpj.FilesLimit
			cp.FileSize = cpj.FileSize
			cp.CacheStorageSize = cpj.CacheStorageSize
			cp.ResultFileName = cpj.ResultFileName
		}
	}
	return cl
}

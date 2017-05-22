package simulator

import "time"

type ConfigList []Config
type ConfigJSONList []ConfigJSON

type Config struct {
	IsTrained            bool
	PeriodDuration       time.Duration
	CooperationThreshold float64
	TestStartPeriod      int
	CachePolicy          cachePolicy
	SimilarityFormula    similarityFormula
	FilesLimit           int
	FileSize             int
	CacheStorageSize     int
}

type ConfigJSON struct {
	IsTrained            bool    `json:"is_trained"`
	PeriodDuration       string  `json:"period_duration"`
	CooperationThreshold float64 `json:"cooperation_threshold"`
	TestStartPeriod      int     `json:"test_start_period"`
	CachePolicy          string  `json:"cache_policy"`
	SimilarityFormula    string  `json:"similarity_formula"`
	FilesLimit           int     `json:"files_limit"`
	FileSize             int     `json:"file_size"`
	CacheStorageSize     int     `json:"cache_storage_size"`
}

func (cjl ConfigJSONList) toConfig() ConfigList {
	var err error
	cl := make(ConfigList, len(cjl))
	for i, cj := range cjl {
		cl[i].IsTrained = cj.IsTrained
		cl[i].PeriodDuration, err = time.ParseDuration(cj.PeriodDuration)
		if err != nil {
			panic(err)
		}
		cl[i].CooperationThreshold = cj.CooperationThreshold
		cl[i].TestStartPeriod = cj.TestStartPeriod

		switch cj.CachePolicy {
		case "leastRecentlyUsed":
			cl[i].CachePolicy = leastRecentlyUsed
		case "leastFrequentlyUsed":
			cl[i].CachePolicy = leastFrequentlyUsed
		}

		switch cj.SimilarityFormula {
		case "exponential":
			cl[i].SimilarityFormula = exponential
		case "cosine":
			cl[i].SimilarityFormula = cosine
		}

		cl[i].FilesLimit = cj.FilesLimit
		cl[i].FileSize = cj.FileSize
		cl[i].CacheStorageSize = cj.CacheStorageSize
	}
	return cl
}

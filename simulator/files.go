package simulator

import "sort"

type popularities map[*file]int
type popularitiesNormalized map[*file]float64
type fileList []*file
type filePopularityList []filePopularity

type filePopularity struct {
	file       *file
	popularity int
}

func (pl periodList) setPopularFiles(files map[string]*file) {
	for pn, p := range pl {
		prd := make(filePopularityList, 0)
		acm := make(filePopularityList, 0)
		for _, f := range files {
			prd = append(prd, filePopularity{f, f.popularityPeriod[pn]})
			acm = append(acm, filePopularity{f, f.popularityAccumulated[pn]})
		}
		sort.Sort(prd)
		sort.Sort(acm)
		p.popularFiles = prd
		p.popularFilesAccumulated = acm
	}
}

func (cs *cacheStorage) setPopularFiles(period int) {
	fpl := make(filePopularityList, 0)
	for f, pop := range cs.popAcm[period] {
		fpl = append(fpl, filePopularity{f, pop})
	}
	sort.Sort(fpl)
	cs.popFiles[period] = fpl
}

func (p popularities) getFileList() fileList {
	fl := make(fileList, 0, len(p))
	for f := range p {
		fl = append(fl, f)
	}
	return fl
}

func (fl fileList) intersect(fl2 fileList) fileList {
	if fl2 == nil {
		return fl
	}
	ifl := make(fileList, 0)
	for _, f := range fl {
		for _, f2 := range fl2 {
			if f == f2 {
				ifl = append(ifl, f)
				break
			}
		}
	}
	return ifl
}

func (fpl filePopularityList) has(f *file) bool {
	if fpl == nil {
		return true
	}
	for _, fp := range fpl {
		if fp.file == f {
			return true
		}
	}
	return false
}

func (fpl filePopularityList) Len() int {
	return len(fpl)
}

func (fpl filePopularityList) Less(i, j int) bool {
	return fpl[i].popularity > fpl[j].popularity
}

func (fpl filePopularityList) Swap(i, j int) {
	fpl[i], fpl[j] = fpl[j], fpl[i]
}

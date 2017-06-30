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
		sort.Stable(prd)
		sort.Stable(acm)
		p.popularFiles = prd.getFileList()
		p.popularFilesAccumulated = acm.getFileList()
	}
}

func (csl cacheStorageList) setPopularFiles(periodID int) {
	for _, cs := range csl {
		prd := make(filePopularityList, 0)
		acm := make(filePopularityList, 0)
		for f, pop := range cs.popularitiesAccumulated[periodID] {
			prd = append(prd, filePopularity{f, cs.popularitiesPeriod[periodID][f]})
			acm = append(acm, filePopularity{f, pop})
		}
		sort.Stable(prd)
		sort.Stable(acm)
		cs.popularFiles[periodID] = prd.getFileList()
		cs.popularFilesAccumulated[periodID] = acm.getFileList()
	}
}

func (p popularities) getFileList() fileList {
	fl := make(fileList, 0, len(p))
	for f := range p {
		fl = append(fl, f)
	}
	return fl
}

func (fpl filePopularityList) getFileList() fileList {
	fl := make(fileList, 0, len(fpl))
	for _, fp := range fpl {
		fl = append(fl, fp.file)
	}
	return fl
}

func (fm fileMap) getFileList() fileList {
	fl := make(fileList, 0, len(fm))
	for _, f := range fm {
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

func (fl fileList) unite(fl2 fileList) fileList {
	if fl2 == nil {
		return fl
	}
	ufl := make(fileList, len(fl))
	copy(ufl, fl)
	for _, f2 := range fl2 {
		isInList := false
		for _, f := range fl {
			if f == f2 {
				isInList = true
				break
			}
		}
		if !isInList {
			ufl = append(ufl, f2)
		}
	}
	return ufl
}

func (fl fileList) has(file *file) bool {
	if fl == nil {
		return true
	}
	for _, f := range fl {
		if f == file {
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

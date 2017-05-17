package sccache

import (
	"math"
	"time"
	"sort"
)

type cachePolicy func(*file, time.Time,*cacheStorage, *cache)

type simFormula func(filePopNorm, filePopNorm) float64

type cacheStorageList []*cacheStorage

type cacheStorage struct {
	smallCells smallCellList
	popAcm []filePop
	caches []*cache
	size int
	space int
}

type cache struct {
	file *file
	size int
	lastReq time.Time
}

func lfu(f *file, t time.Time, cs *cacheStorage, c *cache) {

}

func (pl periodList) simulate(pn int, csl cacheStorageList, scl smallCellList, cp cachePolicy) {
	p := pl[pn]
	for _, r := range p.requests {
		t, f, c := r.time, r.file, r.client
		if c.smallCell == nil {
			csl.assignNewClient(c, f, scl)
			p.newClients = append(p.newClients, c)
		}
		if ok, cf := c.smallCell.cacheStorage.hasFile(f); ok && cf.size == cf.file.size {

		} else {
			cp(f, t, c.smallCell.cacheStorage, cf)
		}
	}
}

func (csl cacheStorageList) assignNewClient(c *client, f *file, scl smallCellList) {
	sclf := csl.smallCellsHasFile(f)
	if len(sclf) != 0 {
		c.assignTo(sclf.leastClients())
	} else {
		c.assignTo(scl.leastClients())
	}
}

func (cs *cacheStorage) hasFile(f *file) (bool, *cache) {
	for _, c := range cs.caches {
		if c.file == f {
			return true, c
		}
	}
	return false, nil
}

func (csl cacheStorageList) smallCellsHasFile(f *file) smallCellList {
	scl := make(smallCellList, 0)
	for _, cs := range csl {
		if ok, _ := cs.hasFile(f); ok {
			scl = append(scl, cs.smallCells...)
			break
		}
	}
	return scl
}

func (scl smallCellList) leastClients() *smallCell {
	sort.Sort(scl)
	return scl[0]
}

func (scl smallCellList) Len() int {
	return len(scl)
}

func (scl smallCellList) Less(i, j int) bool {
	return len(scl[i].clients) < len(scl[j].clients)
}

func (scl smallCellList) Swap(i, j int) {
	scl[i], scl[j] = scl[j], scl[i]
}

func (sc *smallCell) assignTo(cs *cacheStorage) {
	ocs := sc.cacheStorage
	if ocs != nil {
		scl := ocs.smallCells
		for i := range scl {
			if scl[i] == sc {
				scl = append(scl[:i], scl[i + 1:]...)
			}
		}
	}
	cs.smallCells = append(cs.smallCells, sc)
	sc.cacheStorage = cs

	for pn, fp := range sc.popAcm {
		if len(cs.popAcm) - 1 < pn {
			cs.popAcm = append(cs.popAcm, make(filePop))
		}
		for k, v := range fp {
			if ocs != nil {
				ocs.popAcm[pn][k] -= v
			}
			cs.popAcm[pn][k] += v
		}
	}
}

func (scl smallCellList) arrangeCooperation(threshold float64, fn simFormula, pn int) cacheStorageList {
	group := make([]smallCellList, 0)
	if threshold < 0 {
		for _, sc := range scl {
			group = append(group, smallCellList{sc})
		}
	} else {
		ok := make([]bool, len(scl))
		sim := scl.calSimilarity(fn, pn)
		for i := 0; i < len(scl) - 1; i++ {
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
					group[len(group) - 1] = append(group[len(group) - 1], scl[j])
					ok[j] = true
				}
			}
		}
	}

	csl := make(cacheStorageList, len(group))
	for i, g := range group {
		csl[i] = &cacheStorage{smallCells: make(smallCellList, 0)}
		for _, sc := range g {
			sc.assignTo(csl[i])
		}
	}

	return csl
}

func (scl smallCellList) calSimilarity(fn simFormula, pn int) [][]float64 {
	s := make([][]float64, len(scl))
	for i := range s {
		s[i] = make([]float64, len(scl))
	}
	for i, sc := range scl {
		for j := i + 1; j < len(scl); j++ {
			s[i][j] = sc.popAcm[pn].calSimilarity(scl[j].popAcm[pn], fn, nil)
			s[j][i] = s[i][j]
		}
	}
	return s
}

func (fp filePop) calSimilarity(fp2 filePop, fn simFormula, lfl fileList) float64 {
	ifl := fp.getFileList()
	if lfl != nil {
		ifl = ifl.intersection(lfl)
	}
	ifl = ifl.intersection(fp2.getFileList())
	if ifl == nil {
		return 0
	}
	ifp := make(filePop)
	ifp2 := make(filePop)
	for _, f := range ifl {
		ifp[f] = fp[f]
		ifp2[f] = fp2[f]
	}
	return fn(ifp.normalize(), ifp2.normalize())
}

func exponential(fpn1 filePopNorm, fpn2 filePopNorm) float64 {
	var numerator float64
	for k, v := range fpn1 {
		numerator -= v * fpn2[k]
	}
	return 1 - math.Exp(numerator)
}

func cosine(fpn1 filePopNorm, fpn2 filePopNorm) float64 {
	var a, b, ab float64
	for k, v := range fpn1 {
		a += math.Pow(v, 2)
		b += math.Pow(fpn2[k], 2)
		ab += v * fpn2[k]
	}
	return ab / (math.Sqrt(a) * math.Sqrt(b))
}

func (fp filePop) sum() (s int) {
	for _, v := range fp {
		s += v
	}
	return
}

func (fp filePop) normalize() filePopNorm {
	fpn := make(filePopNorm)
	s := fp.sum()
	for k, v := range fp {
		fpn[k] = float64(v) / float64(s)
	}
	return fpn
}

func (fp filePop) getFileList() fileList {
	fl := make([]*file, 0, len(fp))
	for k := range fp {
		fl = append(fl, k)
	}
	return fl
}

func (fl fileList) intersection(fl2 fileList) fileList {
	ifl := make([]*file, 0)
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
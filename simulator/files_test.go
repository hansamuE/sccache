package simulator

import "testing"

var (
	f1  *file                  = new(file)
	f2  *file                  = new(file)
	f3  *file                  = new(file)
	p1  popularities           = popularities{f1: 1, f2: 2, f3: 3}
	p2  popularities           = popularities{f2: 5, f3: 4}
	pn1 popularitiesNormalized = popularitiesNormalized{f1: 0.1, f2: 0.9}
	pn2 popularitiesNormalized = popularitiesNormalized{f1: 0.7, f2: 0.3}
	fl1 fileList
	fl2 fileList
	fpl filePopularityList = filePopularityList{filePopularity{f1, 10}, filePopularity{f2, 5}}
)

func TestPopularity_getFileList(t *testing.T) {
	fl1 = p1.getFileList()
	fl2 = p2.getFileList()
	if len(fl1) != 3 {
		t.Error("popularities.getFileList wrong")
	}
}

func TestFileList_intersect(t *testing.T) {
	if len(fl1.intersect(fl2)) != 2 {
		t.Error("fileList.intersect wrong")
	}
}

func TestCsl_hasFile(t *testing.T) {
	t.Log(csl.smallCellsHasFile(f1))
}

func TestFpl_has(t *testing.T) {
	t.Log(fpl.has(f1))
	t.Log(fpl[:1].has(f1))
	t.Log(fpl[:0].has(f1))
	fpl = nil
	t.Log(fpl.has(f1))
	t.Log(fpl[:0].has(f1))
}

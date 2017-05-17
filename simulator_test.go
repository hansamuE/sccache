package sccache

import "testing"

var (
	f1 *file = new(file)
	f2 *file = new(file)
	f3 *file = new(file)
	fp1 filePop = filePop{f1: 1, f2: 2, f3: 3}
	fp2 filePop = filePop{f2: 5, f3: 4}
	fpn1 filePopNorm = filePopNorm{f1: 0.1, f2: 0.9}
	fpn2 filePopNorm = filePopNorm{f1: 0.7, f2: 0.3}
	fl1 fileList
	fl2 fileList
	csl cacheStorageList
)

func TestArrangeCooperation(t *testing.T) {
	csl = scl.arrangeCooperation(-1, exponential, 2)
	t.Log("csl:", csl)
}

func TestSmallCellList_calSimilarity(t *testing.T) {
	t.Log("scl.calSimilarity:", scl.calSimilarity(exponential, 2))
}

func TestFilePop_calSimilarity(t *testing.T) {
	s := fp1.calSimilarity(fp2, exponential, nil)
	t.Log("filePop.calSimilarity:", s)
}

func TestExponential(t *testing.T) {
	t.Log("exponential:", exponential(fpn1, fpn2))
}

func TestCosine(t *testing.T) {
	t.Log("cosine:", cosine(fpn1, fpn2))
}

func TestFilePop_sum(t *testing.T) {
	if fp1.sum() != 6 {
		t.Error("filePop.sum wrong")
	}
}

func TestFilePop_normalize(t *testing.T) {
	fpn1 = fp1.normalize()
	if fpn1[f3] != 0.5 {
		t.Error("filePop.normalize wrong")
	}
}

func TestFilePop_getFileList(t *testing.T) {
	fl1 = fp1.getFileList()
	fl2 = fp2.getFileList()
	if len(fl1) != 3 {
		t.Error("filePop.getFileList wrong")
	}
}

func TestFileList_intersection(t *testing.T) {
	if len(fl1.intersection(fl2)) != 2 {
		t.Error("fileList.intersection wrong")
	}
}

func TestCsl_hasFile(t *testing.T) {
	t.Log(csl.smallCellsHasFile(f1))
}

func TestCsl_assignNewClient(t *testing.T) {
	csl.assignNewClient(c["1"], f1, scl)
}

func TestSimulate(t *testing.T) {
	p[0].simulate(csl, scl, leastRecentUsed)
	p[2].simulate(csl, scl, leastFreqUsed)
}
package simulator

import "testing"

func TestSmallCellList_calSimilarity(t *testing.T) {
	t.Log("scl.calSimilarity:", smallCells.calSimilarity(exponential, nil))
}

func TestFilePop_calSimilarity(t *testing.T) {
	s := p1.calSimilarity(p2, exponential, nil)
	t.Log("popularities.calSimilarity:", s)
}

func TestExponential(t *testing.T) {
	t.Log("exponential:", exponential(pn1, pn2))
}

func TestCosine(t *testing.T) {
	t.Log("cosine:", cosine(pn1, pn2))
}

func TestFilePop_sum(t *testing.T) {
	if p1.sum() != 6 {
		t.Error("popularities.sum wrong")
	}
}

func TestFilePop_normalize(t *testing.T) {
	pn1 = p1.normalize()
	if pn1[f3] != 0.5 {
		t.Error("popularities.normalize wrong")
	}
}

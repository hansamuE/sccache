package sccache

import "testing"

var (
	fp filePop
	fpn1 filePopNorm
	fpn2 filePopNorm
	f1 *file = new(file)
	f2 *file = new(file)
	f3 *file = new(file)
)

func TestExponential(t *testing.T) {
	fpn1 = filePopNorm{f1: 0.1, f2: 0.9}
	fpn2 = filePopNorm{f1: 0.7, f2: 0.3}
	t.Log("exponential:", exponential(fpn1, fpn2))
}

func TestCosine(t *testing.T) {
	fpn1 = filePopNorm{f1: 0.1, f2: 0.9}
	fpn2 = filePopNorm{f1: 0.7, f2: 0.3}
	t.Log("cosine:", cosine(fpn1, fpn2))
}

func TestFilePop_sum(t *testing.T) {
	fp = filePop{f1: 1, f2: 2, f3: 3}
	if fp.sum() != 6 {
		t.Error("filePop.sum wrong")
	}
}

func TestFilePop_normalize(t *testing.T) {
	fpn1 = fp.normalize()
	if fpn1[f3] != 0.5 {
		t.Error("filePop.normalize wrong")
	}
	t.Log(fpn1)
}
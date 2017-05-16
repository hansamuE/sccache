package sccache

import "testing"

var (
	fp filePop
	fpn filePopNorm
	f1 *file = new(file)
	f2 *file = new(file)
	f3 *file = new(file)
)

func TestFilePop_sum(t *testing.T) {
	fp = filePop{f1: 1, f2: 2, f3: 3}
	if fp.sum() != 6 {
		t.Error("filePop.sum wrong")
	}
}

func TestFilePop_normalize(t *testing.T) {
	fpn = fp.normalize()
	if fpn[f3] != 0.5 {
		t.Error("filePop.normalize wrong")
	}
	t.Log(fpn)
}
package sccache

import "math"

func exponential(fpn1 filePopNorm, fpn2 filePopNorm) float64 {
	var numerator float64
	for k, v := range fpn1{
		numerator -= v * fpn2[k]
	}
	numerator = math.Exp(numerator)
	return 1 - numerator
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
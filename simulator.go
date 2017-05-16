package sccache

import "math"

type simFormula func(filePopNorm, filePopNorm) float64

func (fp filePop) calSimilarity(fps []filePop, fn simFormula, lfl fileList) []float64 {
	s := make([]float64, len(fps))
	ifl := fp.getFileList()
	if lfl != nil {
		ifl = ifl.intersection(lfl)
	}
	for i, fp2 := range fps {
		ifl = ifl.intersection(fp2.getFileList())
		if ifl == nil {
			s[i] = 0
			continue
		}
		ifp := make(filePop)
		ifp2 := make(filePop)
		for _, f := range ifl {
			ifp[f] = fp[f]
			ifp2[f] = fp2[f]
		}
		s[i] = fn(ifp.normalize(), ifp2.normalize())
	}
	return s
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
package sccache

import "math"

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
	for i := range fl {
		for j := range fl2 {
			if fl[i] == fl2[j] {
				ifl = append(ifl, fl[i])
				break
			}
		}
	}
	return ifl
}
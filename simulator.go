package sccache

//func bigclam(p1 map[*file]int, p2 map[*file]int) float64 {
//
//}

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
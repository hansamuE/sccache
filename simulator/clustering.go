package simulator

import (
	"github.com/cdipaolo/goml/base"
	"github.com/cdipaolo/goml/cluster"
)

const maxIterations = 50

var clusteringModel *cluster.KMeans

func (pl periodList) clustering(clusterNum int) (clientList, []int) {
	trainingSet, trainingClientList := pl.getClientFilePopularity()
	clusteringModel = cluster.NewKMeans(clusterNum, maxIterations, trainingSet)
	if clusteringModel.Learn() != nil {
		panic("Clustering error!")
	}
	return trainingClientList, clusteringModel.Guesses()
}

func (pl periodList) getClientList() clientList {
	ucm := make(clientMap)
	for _, p := range pl {
		for cid, c := range p.clients {
			ucm[cid] = c
		}
	}
	cl := make(clientList, 0, len(ucm))
	for _, c := range ucm {
		cl = append(cl, c)
	}
	return cl
}

func (pl periodList) getClientFilePopularity() ([][]float64, clientList) {
	cl := pl.getClientList()
	data := make([][]float64, len(cl))
	for i, c := range cl {
		data[i] = make([]float64, len(filesList))
		for j, f := range filesList {
			pop := 0
			if popEnd, ok := c.popularityAccumulated[pl[len(pl)-1].id][f]; ok {
				pop = popEnd
				if popStart, ok := c.popularityAccumulated[pl[0].id][f]; ok {
					pop -= popStart
					pop += c.popularityPeriod[pl[0].id][f]
				}
			}
			data[i][j] = float64(pop)
		}
	}
	base.Normalize(data)
	return data, cl
}

func (c *client) getFilePopularity() []float64 {
	data := make([]float64, len(filesList))
	for i, f := range filesList {
		pop := 0
		if popAcc, ok := c.popularityAccumulated[periodNo][f]; ok {
			pop = popAcc
		}
		data[i] = float64(pop)
	}
	base.NormalizePoint(data)
	return data
}

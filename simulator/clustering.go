package simulator

import (
	"github.com/cdipaolo/goml/base"
	"github.com/cdipaolo/goml/cluster"
)

const maxIterations = 50

var clusteringModel *cluster.KMeans

func clustering(pl periodList, clusterNum int) (clientList, []int) {
	centroids := make([][]float64, clusterNum)
	for i := 0; i < clusterNum; i++ {
		centroids[i] = make([]float64, clusterNum-2)
	}
	for i := 0; i < clusterNum-2; i++ {
		for j := 0; j < clusterNum-2; j++ {
			if j == i {
				centroids[i][j] = 1
			} else {
				centroids[i][j] = 0
			}
		}
	}
	for i := 0; i < clusterNum-2; i++ {
		centroids[clusterNum-2][i] = 1.0 / float64(clusterNum-2) * cluThreshold
		centroids[clusterNum-1][i] = 0
	}
	clusteringModel = cluster.NewKMeans(clusterNum, maxIterations, nil)
	clusteringModel.Centroids = centroids
	trainingSet, trainingClientList := pl.getClientFilePopularity()
	sum := 0
	for _, f := range filesList {
		sum += f.popularityAccumulated[pl[len(pl)-1].id]
	}
	reqThreshold = sum / len(trainingClientList)
	guesses := make([]int, len(trainingClientList))
	for i, data := range trainingSet {
		total := 0
		for _, pop := range trainingClientList[i].popularityAccumulated[pl[len(pl)-1].id] {
			total += pop
		}
		if total < reqThreshold {
			guesses[i] = clusterNum - 1
			continue
		}
		guess, err := clusteringModel.Predict(data[:clusterNum-2])
		if err != nil {
			panic("Prediction error!")
		}
		guesses[i] = int(guess[0])
	}
	//clusteringModel = cluster.NewKMeans(clusterNum, maxIterations, trainingSet)
	//if clusteringModel.Learn() != nil {
	//	panic("Clustering error!")
	//}
	//guesses := clusteringModel.Guesses()

	smallCells = newSmallCells(clusterNum)
	for i, c := range trainingClientList {
		c.assignTo(smallCells[guesses[i]])
	}

	return trainingClientList, guesses
}

func onlineLearn(alpha float64, cl clientList) {
	clusteringModel.UpdateLearningRate(alpha)
	stream := make(chan base.Datapoint)
	errors := make(chan error)
	guess := make(chan int)
	go clusteringModel.OnlineLearn(errors, stream, func(theta [][]float64) {
		guess <- int(theta[0][0])
	})
	go func() {
		for _, c := range cl {
			stream <- base.Datapoint{X: c.getFilePopularity(periods[:periodNo+1])}
			c.assignTo(smallCells[<-guess])
		}
		close(stream)
	}()
	err, more := <-errors
	if err != nil || more != false {
		panic("Online Learning error!")
	}
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
		data[i] = c.getFilePopularity(pl)
	}
	return data, cl
}

func (c *client) getFilePopularity(pl periodList) []float64 {
	data := make([]float64, len(filesList))
	//for i, f := range filesList {
	for i, f := range pl[len(pl)-1].popularFilesAccumulated {
		pop := 0
		if popEnd, ok := c.popularityAccumulated[pl[len(pl)-1].id][f]; ok {
			pop = popEnd
			if popStart, ok := c.popularityAccumulated[pl[0].id][f]; ok {
				pop -= popStart
				pop += c.popularityPeriod[pl[0].id][f]
			}
		}
		data[i] = float64(pop)
	}
	base.NormalizePoint(data)
	return data
}

func clusteringWithSimilarity(pl periodList, clusterNum int) (clientList, []int) {
	trainingSet, trainingClientList := pl.getClientsSimilarity()
	clusteringModel = cluster.NewKMeans(clusterNum, maxIterations, trainingSet)
	if clusteringModel.Learn() != nil {
		panic("Clustering error!")
	}
	guesses := clusteringModel.Guesses()

	smallCells = newSmallCells(len(clusteringModel.Centroids))
	for i, c := range trainingClientList {
		c.assignTo(smallCells[guesses[i]])
	}

	return trainingClientList, guesses
}

func (pl periodList) getClientsSimilarity() ([][]float64, clientList) {
	cl := pl.getClientList()
	data := make([][]float64, len(cl))
	cp := make([]popularities, len(cl))
	for i, c := range cl {
		cp[i] = make(popularities)
		for f, pop := range c.popularityAccumulated[pl[len(pl)-1].id] {
			if popStart, ok := c.popularityAccumulated[pl[0].id][f]; ok {
				pop -= popStart
				pop += c.popularityPeriod[pl[0].id][f]
			}
			cp[i][f] = pop
		}
	}

	for i := range cl {
		data[i] = make([]float64, len(cl))
	}
	for i := range cl {
		for j := range cl {
			data[i][j] = cp[i].calSimilarity(cp[j], nil)
			data[j][i] = data[i][j]
		}
	}
	//base.Normalize(data)
	return data, cl
}

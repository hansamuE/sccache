package predictor

import (
	"errors"
	"math"

	"github.com/gonum/stat"
)

type gma struct {
	predictor
	ow int
}

func NewGMA(name string, ow int) *gma {
	ama := new(gma)
	ama.name = name
	ama.ow = ow

	return ama
}

func calGeometricalAverageC(input []int, t int, ow int) float64 {
	t0 := t - ow + 1
	if t0 < 1 {
		t0 = 1
	}
	x := make([]float64, 0)
	for i := t0; i < t+1; i++ {
		c := calC(input, i)
		if c == 0.0 {
			c = 1.0
		}
		x = append(x, c)
	}
	return stat.GeometricMean(x, nil)
}

func (p *gma) Predict(input []int) ([]int, error) {
	if len(input) < 2 {
		return nil, errors.New("no sufficient data to predict")
	}

	output := make([]int, len(input)+1)
	output[0] = input[0]
	output[1] = input[0]

	for t := 1; t < len(input); t++ {
		c := calGeometricalAverageC(input, t, p.ow)
		output[t+1] = input[t] + int(math.Floor(c*float64(input[t]-input[t-1])+0.5))
	}

	return output, nil
}

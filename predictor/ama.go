package predictor

import (
	"errors"
	"math"

	"github.com/gonum/stat"
)

type ama struct {
	predictor
	ow int
}

func NewAMA(name string, ow int) *ama {
	ama := new(ama)
	ama.name = name
	ama.ow = ow

	return ama
}

func calArithmeticalAverageC(input []int, t int, ow int) float64 {
	t0 := t - ow + 1
	if t0 < 1 {
		t0 = 1
	}
	x := make([]float64, 0)
	for i := t0; i < t+1; i++ {
		x = append(x, calC(input, i))
	}
	return stat.Mean(x, nil)
}

func (p *ama) Predict(input []int) ([]int, error) {
	if len(input) < 2 {
		return nil, errors.New("no sufficient data to predict")
	}

	output := make([]int, len(input)+1)
	output[0] = input[0]
	output[1] = input[0]

	for t := 1; t < len(input); t++ {
		c := calArithmeticalAverageC(input, t, p.ow)
		output[t+1] = input[t] + int(math.Floor(c*float64(input[t]-input[t-1])+0.5))
	}

	return output, nil
}

package predictor

import (
	"errors"
	"math"
)

type cb struct {
	predictor
	c float64
}

func NewCB(name string, c float64) *cb {
	cb := new(cb)
	cb.name = name
	cb.c = c

	return cb
}

func (p *cb) Predict(input []int) ([]int, error) {
	if len(input) < 2 {
		return nil, errors.New("No sufficient data to predict.")
	}

	output := make([]int, len(input)+1)
	output[0] = input[0]
	output[1] = input[0]

	for t := 1; t < len(input); t++ {
		output[t+1] = input[t] + int(math.Floor(p.c*float64(input[t]-input[t-1])+0.5))
	}

	return output, nil
}

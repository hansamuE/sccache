package predictor

import (
	"errors"
	"math"
)

type db struct {
	predictor
}

func NewDB(name string) *db {
	db := new(db)
	db.name = name

	return db
}

func calC(input []int, t int) float64 {
	d := float64(input[t-1])
	if t >= 2 {
		d -= float64(input[t-2])
	}
	if d == 0.0 {
		d = 1.0
	}

	return float64(input[t]-input[t-1]) / d
}

func (p *db) Predict(input []int) ([]int, error) {
	if len(input) < 2 {
		return nil, errors.New("no sufficient data to predict")
	}

	output := make([]int, len(input)+1)
	output[0] = input[0]
	output[1] = input[0]

	for t := 1; t < len(input); t++ {
		c := calC(input, t)
		output[t+1] = input[t] + int(math.Floor(c*float64(input[t]-input[t-1])+0.5))
	}

	return output, nil
}

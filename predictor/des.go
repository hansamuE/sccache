package predictor

import (
	"errors"
	"math"
)

type des struct {
	predictor
	alpha float64
}

func NewDES(name string, alpha float64) *des {
	des := new(des)
	des.name = name
	des.alpha = alpha

	return des
}

func (p *des) Predict(input []int) ([]int, error) {
	if len(input) < 2 {
		return nil, errors.New("No sufficient data to predict.")
	}

	output := make([]int, len(input)+1)
	output[0] = input[0]
	output[1] = input[0]

	Sp := float64(input[0])
	Sdp := float64(input[0])
	for t := 1; t < len(input); t++ {
		Sp = p.alpha*float64(input[t]) + (1-p.alpha)*Sp
		Sdp = p.alpha*Sp + (1-p.alpha)*Sdp
		L := 2*Sp - Sdp
		T := (p.alpha / (1 - p.alpha)) * (Sp - Sdp)
		output[t+1] = int(math.Floor(L + T + 0.5))
	}

	return output, nil
}

package predictor

type predictor struct {
	name string
}

type Predictors interface {
	Name() string
	Predict(input []int) ([]int, error)
}

func (p *predictor) Name() string {
	return p.name
}

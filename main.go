package main

import (
	"os"
	"strconv"

	"github.com/hansamuE/sccache/filter"
	"github.com/hansamuE/sccache/generator"
	"github.com/hansamuE/sccache/simulator"
)

func main() {
	args := os.Args
	if len(args) == 1 {
		return
	}
	switch args[1] {
	case "sim":
		if len(args) < 3 {
			return
		}
		var path string
		if len(args) < 4 {
			path = ""
		} else {
			path = args[2]
		}
		simulator.Simulate(path, args[len(args)-1])
	case "gen":
		if len(args) < 5 {
			return
		}
		userNum, err := strconv.Atoi(args[3])
		if err != nil {
			panic(err)
		}
		requestProportion, err := strconv.ParseFloat(args[4], 64)
		if err != nil {
			panic(err)
		}
		generator.GenerateRequests(args[2], userNum, requestProportion)
	case "fil":
		err, path, inputFileName, comma, column, isURL, fileLimit, timeThreshold := filter.ReadArgs(args)
		if err != nil {
			return
		}
		filter.FilterLog(path, inputFileName, comma, column, isURL, fileLimit, timeThreshold)
	default:
		return
	}
}

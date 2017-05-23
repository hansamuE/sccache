package main

import (
	"os"
	"strconv"

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
		var path string
		if len(args) < 3 {
			path = ""
		} else {
			path = args[2]
		}
		simulator.Simulate(path)
	case "gen":
		if len(args) < 4 {
			return
		}
		userNum, err := strconv.Atoi(args[3])
		if err != nil {
			panic(err)
		}
		generator.GenerateRequests(args[2], userNum)
	default:
		return
	}
}

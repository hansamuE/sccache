package main

import (
	"os"

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
	default:
		return
	}
}

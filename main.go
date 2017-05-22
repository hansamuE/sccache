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
		simulator.Simulate()
	}
}

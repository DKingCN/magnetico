package main

import (
	"flag"
	"fmt"
	"os"
)

var UsageText = `
Usage:
	magtool  COMMAND  arguments...
	magtool  COMMAND  --help

COMMAND:
	migrate
`

func main() {
	flag.Usage = func() {
		_, _ = fmt.Fprint(os.Stderr, UsageText)
		flag.PrintDefaults()
	}
	flag.Parse()
	if len(flag.Args()) == 0 {
		flag.Usage()
		return
	}

	switch os.Args[1] {
	case "migrate":
		migrate()
	default:
		flag.Usage()
	}
}

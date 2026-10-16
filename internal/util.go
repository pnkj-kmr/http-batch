package internal

import (
	"flag"
	"log"
	"os"
)

func GetCmdPipe() CmdPipe {
	version := flag.Bool("version", false, "Application version")
	fileName := flag.String("f", "input.json", "Give a input file name")
	outDir := flag.String("o", ".", "Output directory")
	noWorkers := flag.Int("w", 4, "Number of workers")
	timeout := flag.Int("t", 30, "Timeout [secs]")

	flag.Parse()

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatal("Error while creating output directory ", err)
	}
	log.Println("File accepted:", *fileName, "| output file:", *outDir)
	log.Println("timeout:", *timeout, "| workers:", *noWorkers)

	return CmdPipe{
		IFile: *fileName, ODir: *outDir, Workers: *noWorkers,
		Timeout: *timeout, Version: *version,
	}
}

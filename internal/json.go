package internal

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
)

type _json struct {
	ifile   string
	odir    string
	count   int
	timeout int
}

func NewJSON(ifile, odir string, count, timeout int) HTTPBatch {
	return &_json{ifile, odir, count, timeout}
}

func (c *_json) GetFlow() (out []Flow) {
	fileData, err := os.ReadFile(c.ifile)
	if err != nil {
		log.Fatal("Error while read the json file", err)
	}

	err = json.Unmarshal(fileData, &out)
	if err != nil {
		log.Fatal("Error while converting to json", err)
	}

	if len(out) == 0 {
		log.Fatal("No flow found in the json file ", c.ifile)
	}

	return out
}

func (c *_json) ProduceOutput(ch <-chan FlowResult, exitCh chan<- struct{}) {
	var out []FlowResult
	for r := range ch {
		out = append(out, r)
	}

	outJson, _ := json.MarshalIndent(out, "", "  ")
	err := os.WriteFile(filepath.Join(c.odir, "summary.json"), outJson, 0644)
	if err != nil {
		log.Fatal("Error while writing into file", err)
	}

	exitCh <- struct{}{}
}

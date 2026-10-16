package main

import (
	_ "embed"
	"fmt"
	"http-batch/internal"
	"log"
	"sync"
	"time"
)

//go:embed version.txt
var appVersion string

func main() {
	st := time.Now()

	cmdPipe := internal.GetCmdPipe()
	if cmdPipe.Version {
		log.Println("Version: ", appVersion)
		return
	}

	// // every run get its own directory, so the previous output is kept
	// runDir := filepath.Join(cmdPipe.ODir, "run-"+st.Format("20060102-150405"))
	// if err := os.MkdirAll(runDir, 0755); err != nil {
	// 	log.Fatal("Error while creating output directory ", err)
	// }
	// log.Println("Output directory -- ", runDir)
	// var batch internal.HTTPBatch = internal.NewJSON(cmdPipe.IFile, runDir, cmdPipe.Workers, cmdPipe.Timeout)

	var batch internal.HTTPBatch = internal.NewJSON(cmdPipe.IFile, cmdPipe.ODir, cmdPipe.Workers, cmdPipe.Timeout)

	flows := batch.GetFlow()
	log.Println("Total flows -- ", len(flows))

	exitCh := make(chan struct{})
	ch := make(chan internal.FlowResult, cmdPipe.Workers)
	go batch.ProduceOutput(ch, exitCh)

	var wg sync.WaitGroup
	c := make(chan int, cmdPipe.Workers)
	for i := 0; i < len(flows); i++ {
		wg.Add(1)
		c <- 1
		go func(f internal.Flow, timeout int) {
			defer func() { wg.Done(); <-c }()
			out, err := internal.Call(f, internal.Options{Timeout: timeout, OutDir: cmdPipe.ODir, Workers: cmdPipe.Workers})
			if err == nil {
				err = fmt.Errorf("")
			}
			ch <- out
		}(flows[i], cmdPipe.Timeout)
		log.Println("Flow sent for execution -- ", flows[i].Name, i+1)
		// log.Println("Flow sent for execution -- ", flows[i])
	}

	wg.Wait()
	close(ch)
	<-exitCh
	log.Println("Execution completed. time taken", time.Since(st))
}

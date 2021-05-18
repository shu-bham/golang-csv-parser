package main

import (
	"os"
	"sync"

	"example.com/rainparser"
)

func main() {
	filePaths := os.Args[1:]
	empChan := make(chan rainparser.Employee)
	var wg [2]sync.WaitGroup
	cols := make(map[string]bool)
	var mu sync.Mutex
	for _, filePath := range filePaths {
		wg[0].Add(1)
		go rainparser.ProcessCsv(filePath, empChan, &wg, cols, &mu)
	}
	go func() {
		wg[0].Wait()
		close(empChan)
	}()

	rainparser.WriteData(empChan, "output.csv")
}

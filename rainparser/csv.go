package rainparser

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
)

/**
	Entry method for each csv file
 	actions performed:
	1) open file in read mode
	2) process the column labels
	3) create and push the employee data onto channel
*/
func ProcessCsv(filePath string, empChan chan Employee, wg *[2]sync.WaitGroup, columnSet map[string]bool, mu *sync.Mutex) {
	defer wg[0].Done()
	inputFile, err := os.Open(filePath)
	defer inputFile.Close()
	if err != nil {
		log.Print(err)
		return
	}
	log.Printf(">>>> Processing started : %s", filePath)
	csvReader := csv.NewReader(inputFile)
	wg[1].Add(1)
	columnLabels, ok := processFirstLine(filePath, csvReader, &wg[1])
	if !ok {
		return
	}
	updateColumns(columnLabels, mu, columnSet)
	wg[1].Wait()
	processRecords(filePath, csvReader, columnLabels, columnSet, empChan)
}

// Utility method to collect column names
func updateColumns(labels []string, mu *sync.Mutex, columnSet map[string]bool) {
	for _, col := range labels {
		mu.Lock()
		columnSet[col] = true
		mu.Unlock()
	}
}

/**
This method reads employee records from the Employee channel and
write onto the output file
*/
func WriteData(ch chan Employee, filePath string) {
	outFile, err := os.Create(filePath)
	defer outFile.Close()

	if err != nil {
		log.Fatal(err)
	}
	csvWriter := csv.NewWriter(outFile)
	defer csvWriter.Flush()
	columnNameWritten := false
	var cols, vals []string
	ctr := 0
	for {
		emp, isOpen := <-ch
		if isOpen {
			log.Println("Writing to file:", emp.info)
			if !columnNameWritten {
				for key := range emp.info {
					cols = append(cols, key)
				}
			}

			if !columnNameWritten {
				csvWriter.Write(cols)
				columnNameWritten = true
			} else {
				for _, v := range cols {
					vals = append(vals, emp.info[v])
				}
				csvWriter.Write(vals)
				ctr++
				vals = nil
			}

		} else {
			log.Printf("Completed writing records to file : %s, Record Count : %v", filePath, ctr)
			break
		}

	}
}

/**
Get column names from a file
and change as per the regex provided for employee
*/
func processFirstLine(filePath string, csvReader *csv.Reader, wg *sync.WaitGroup) (columnLabels []string, res bool) {
	defer wg.Done()
	line, err := csvReader.Read()
	if err == io.EOF {
		log.Printf("<<<< Reached EOF : %s", filePath)
		res = false
		return
	}
	if err != nil {
		log.Printf("Error reading file : %s", err)
		res = false
		return
	}
	for _, colName := range line {
		s := strings.TrimSpace(colName)
		matched := false
		for parentCol, regexExpre := range ColumnLabelRegex {
			matched, err = regexp.MatchString(regexExpre, s)
			if err != nil {
				log.Println(err)
				return
			}
			if matched {
				columnLabels = append(columnLabels, parentCol)
				break
			}
		}
		// when new column is encountered
		if !matched {
			columnLabels = append(columnLabels, s)
		}

	}

	res = true
	return
}

/**
Method used for transforming and pushing records from csv onto the provided channel
*/
func processRecords(filePath string, csvReader *csv.Reader, columnLabels []string, columnSet map[string]bool, empChan chan Employee) {
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			log.Printf("<<<< Reached EOF : %s", filePath)
			break
		}
		if err != nil {
			log.Printf("Error reading file : %s", err)
			break
		}
		mymap := make(map[string]string)
		for i, val := range line {
			mymap[columnLabels[i]] = val
		}
		e := Employee{info: mymap, srcFile: filePath, isValid: true}
		e.Standardise(columnSet)
		empChan <- e
	}
}

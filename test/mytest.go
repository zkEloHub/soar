package main

import (
	"io/ioutil"
	"log"
	"strings"
)

var fileName = "../cmd/soar/data.sql"
var outFileName = "../cmd/soar/outdata.sql"

func filterDML() {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalln(err)
	}
	lines := strings.Split(string(data), "\n")

	outData := strings.Join(lines, ";\n")
	err = ioutil.WriteFile(outFileName, []byte(outData), 0666)
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	filterDML()
}

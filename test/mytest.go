package main

import "fmt"

func main() {
	var factor = 0.5

	offset := int64(float64(100) / factor)
	fmt.Println(offset)
}

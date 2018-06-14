package main

import (
	"fmt"
	"time"
)

func main() {
	startTime := time.Now()

	elapsed := time.Now().Sub(startTime)
	fmt.Printf("Total migration time: %v\n", elapsed)
}

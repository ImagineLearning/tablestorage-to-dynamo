package main

import (
	"log"
	"time"

	"github.com/ImagineLearning/tablestorage-to-dynamo/internal/app/migration"
)

func main() {

	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Println("Beginning migration")
	startTime := time.Now()

	config := migration.LoadMigrationConfig()
	migration := migration.NewMigration(config)
	migration.Start()

	elapsed := time.Now().Sub(startTime)
	log.Printf("Total migration time: %v\n", elapsed)
}

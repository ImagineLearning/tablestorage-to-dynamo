package migration

import (
	"fmt"
	"os"
	"sync"

	"github.com/kelseyhightower/envconfig"
	dp "github.com/tablestorage-to-dynamo/internal/pkg/dataprovider"
)

type MigrationConfig struct {
	Dynamo          dp.DynamoConfig
	TableStorage    dp.TableStorageConfig
	NumWorkers      int `default:"100"`
	StartDateOffset int `required:"true"`
	EndDateOffset   int `required:"true"`
	BufferSize      int `default:"500"`
}

func LoadMigrationConfig() MigrationConfig {
	var config MigrationConfig

	var dynamoConfig dp.DynamoConfig
	err := envconfig.Process("DYNAMO", &dynamoConfig)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	config.Dynamo = dynamoConfig

	var tsConfig dp.TableStorageConfig
	err = envconfig.Process("TABLESTORAGE", &tsConfig)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	config.TableStorage = tsConfig

	err = envconfig.Process("", &config)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	return config
}

type Migration struct {
	TableStorage    dp.TableStorageProvider
	Dynamo          dp.DynamoProvider
	ReadWorkQueue   dp.TableStorageReadWork
	ReadWorkerPool  chan dp.TableStorageReadWork
	WriteWorkQueue  dp.DynamoWriteWork
	WriteWorkerPool chan dp.DynamoWriteWork
	Config          MigrationConfig
	WaitGrp         *sync.WaitGroup
}

// NewMigration returns a migration which has the table storage table, work queue, wait group, etc
func NewMigration(migrationConfig MigrationConfig) Migration {

	return Migration{
		TableStorage:    dp.NewTableStorageProvider(migrationConfig.TableStorage),
		Dynamo:          dp.NewDynamoProvier(migrationConfig.Dynamo),
		ReadWorkQueue:   make(dp.TableStorageReadWork, migrationConfig.BufferSize),
		ReadWorkerPool:  make(chan dp.TableStorageReadWork, migrationConfig.NumWorkers),
		WriteWorkQueue:  make(dp.DynamoWriteWork, migrationConfig.BufferSize),
		WriteWorkerPool: make(chan dp.DynamoWriteWork, migrationConfig.NumWorkers),
		Config:          migrationConfig,
		WaitGrp:         new(sync.WaitGroup),
	}
}

// Start starts migrating data from table storage to dynamo using a dispatch, worker pool, work queue pattern
func (migration *Migration) Start() {

	// Create and start workers
	for i := 0; i < migration.Config.NumWorkers; i++ {
		readWorker := dp.NewTableStorageReadWorker(i+1, migration.ReadWorkerPool)
		readWorker.Start(&migration.TableStorage, migration.WriteWorkQueue, migration.WaitGrp)

		writeWorker := dp.NewDynamoWriteWorker(i+1, migration.WriteWorkerPool)
		writeWorker.Start(&migration.Dynamo, &migration.Config.TableStorage.ColumnNames, migration.WaitGrp)
	}

	// Dispatch work
	go func() {
		for {
			select {
			case readWork := <-migration.ReadWorkQueue:
				go func() {
					worker := <-migration.ReadWorkerPool
					worker <- readWork
				}()
			case writeWork := <-migration.WriteWorkQueue:
				go func() {
					worker := <-migration.WriteWorkerPool
					worker <- writeWork
				}()
			}
		}
	}()

	// Generate work
	for i := migration.Config.StartDateOffset; i > 0; i-- {
		migration.WaitGrp.Add(1)
		migration.ReadWorkQueue <- dp.NewDateRange(i, 1)
	}

	// Wait for work to be completed
	migration.WaitGrp.Wait()
}

//Undo deletes data from table storage in dynamo, or in other words, undos the migration.
func (migration *Migration) Undo() {

	// Create and start workers
	for i := 0; i < migration.Config.NumWorkers; i++ {
		readWorker := dp.NewTableStorageReadWorker(i+1, migration.ReadWorkerPool)
		readWorker.Start(&migration.TableStorage, migration.WriteWorkQueue, migration.WaitGrp)

		writeWorker := dp.NewDynamoWriteWorker(i+1, migration.WriteWorkerPool)
		writeWorker.StartDelete(&migration.Dynamo, migration.WaitGrp)
	}

	// Dispatch work
	go func() {
		for {
			select {
			case readWork := <-migration.ReadWorkQueue:
				go func() {
					worker := <-migration.ReadWorkerPool
					worker <- readWork
				}()
			case writeWork := <-migration.WriteWorkQueue:
				go func() {
					worker := <-migration.WriteWorkerPool
					worker <- writeWork
				}()
			}
		}
	}()

	// Generate work
	for i := migration.Config.StartDateOffset; i > migration.Config.EndDateOffset; i-- {
		migration.WaitGrp.Add(1)
		migration.ReadWorkQueue <- dp.NewDateRange(i, 1)
	}

	// Wait for work to be completed
	migration.WaitGrp.Wait()
}

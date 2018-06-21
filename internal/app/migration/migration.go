package migration

import (
	"log"
	"os"
	"sync"

	"github.com/kelseyhightower/envconfig"
	dp "github.com/tablestorage-to-dynamo/internal/pkg/dataprovider"
)

type MigrationConfig struct {
	Dynamo       dp.DynamoConfig
	TableStorage dp.TableStorageConfig
	NumWorkers   int `default:"100"`
	BufferSize   int `default:"500"`
	Ranges       []string
}

func LoadMigrationConfig() MigrationConfig {
	var config MigrationConfig

	var dynamoConfig dp.DynamoConfig
	err := envconfig.Process("DYNAMO", &dynamoConfig)

	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	config.Dynamo = dynamoConfig

	var tsConfig dp.TableStorageConfig
	err = envconfig.Process("TABLESTORAGE", &tsConfig)

	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	config.TableStorage = tsConfig

	err = envconfig.Process("", &config)

	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	return config
}

type Migration struct {
	TableStorage    dp.TableStorageProvider
	Dynamo          dp.DynamoProvider
	Status          dp.DynamoProvider
	ReadWorkQueue   dp.TableStorageReadWork
	ReadWorkerPool  chan dp.TableStorageReadWork
	WriteWorkQueue  dp.DynamoWriteWork
	WriteWorkerPool chan dp.DynamoWriteWork
	Config          MigrationConfig
	WaitGrp         *sync.WaitGroup
}

// NewMigration returns a migration which has the table storage table, work queue, wait group, etc
func NewMigration(migrationConfig MigrationConfig) Migration {
	statusProvider := dp.NewDynamoProvier(migrationConfig.Dynamo)
	statusProvider.NewMigrationStatusTable()

	return Migration{
		TableStorage:    dp.NewTableStorageProvider(migrationConfig.TableStorage),
		Dynamo:          dp.NewDynamoProvier(migrationConfig.Dynamo),
		Status:          statusProvider,
		ReadWorkQueue:   make(dp.TableStorageReadWork, migrationConfig.BufferSize),
		ReadWorkerPool:  make(chan dp.TableStorageReadWork, migrationConfig.NumWorkers),
		WriteWorkQueue:  make(dp.DynamoWriteWork, migrationConfig.BufferSize),
		WriteWorkerPool: make(chan dp.DynamoWriteWork, migrationConfig.NumWorkers),
		Config:          migrationConfig,
		WaitGrp:         new(sync.WaitGroup),
	}
}

func QueryRangeHasBeenMigrated(alreadyMigrated []dp.QueryRange, queryRange dp.QueryRange) bool {
	for _, value := range alreadyMigrated {
		if value.Ge == queryRange.Ge && value.Lt == queryRange.Lt {
			return true
		}
	}
	return false
}

func (migration *Migration) GenerateRanges(ranges chan string) chan string {
	newRanges := make(chan string, 10000)

	if len(ranges) == 0 {
		for _, hexCode := range migration.Config.Ranges {
			ranges <- hexCode
		}
		return ranges
	}

	close(ranges)
	for elem := range ranges {
		for _, hexCode := range migration.Config.Ranges {
			newRanges <- elem + hexCode
		}
	}
	return newRanges
}

func (migration *Migration) DispatchReadWork(alreadyMigrated []dp.QueryRange) {

	rangeQueue := make(chan string, 10000)

	for currentPrecision, maxPrecision := 0, 5; currentPrecision < maxPrecision; currentPrecision++ {
		rangeQueue = migration.GenerateRanges(rangeQueue)
	}
	close(rangeQueue)

	ge := <-rangeQueue
	queryRange := dp.QueryRange{Ge: ge}

	for lt := range rangeQueue {
		queryRange.Lt = lt

		if !QueryRangeHasBeenMigrated(alreadyMigrated, queryRange) {
			migration.WaitGrp.Add(1)
			migration.ReadWorkQueue <- queryRange
		}

		queryRange.Ge = lt
	}
}

// Start stars migrating data from table storage to dynamo using a dispatch, worker pool, work queue pattern
func (migration *Migration) Start() {

	// Create and start workers
	for i := 0; i < migration.Config.NumWorkers; i++ {
		readWorker := dp.NewTableStorageReadWorker(i+1, migration.ReadWorkerPool)
		readWorker.Start(&migration.TableStorage, &migration.Status, migration.WriteWorkQueue, migration.WaitGrp)

		writeWorker := dp.NewDynamoWriteWorker(i+1, migration.WriteWorkerPool)
		writeWorker.Start(&migration.Dynamo, &migration.Status, &migration.Config.TableStorage.ColumnNames, migration.WaitGrp)
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

	alreadyMigrated := migration.Status.ScanStatusTable()

	// Create and dispatch read work
	migration.DispatchReadWork(alreadyMigrated)

	// Wait for work to be completed
	migration.WaitGrp.Wait()

	// Clean up status table if migration completed.
	// migration.Status.DeleteMigrationStatusTable()
}

//Undo deletes data from table storage in dynamo, or in other words, undos the migration.
func (migration *Migration) Undo() {

	// Create and start workers
	for i := 0; i < migration.Config.NumWorkers; i++ {
		readWorker := dp.NewTableStorageReadWorker(i+1, migration.ReadWorkerPool)
		readWorker.Start(&migration.TableStorage, &migration.Status, migration.WriteWorkQueue, migration.WaitGrp)

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

	// Create and dispatch read work
	migration.DispatchReadWork([]dp.QueryRange{})

	// Wait for work to be completed
	migration.WaitGrp.Wait()
}

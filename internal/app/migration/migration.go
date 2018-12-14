package migration

import (
	"log"
	"os"
	"sync"

	dp "github.com/ImagineLearning/tablestorage-to-dynamo/internal/pkg/dataprovider"
	"github.com/kelseyhightower/envconfig"
)

var (
	hexCodes = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}
)

// Config represents all config values needed for a migration.
type Config struct {
	Dynamo         dp.DynamoConfig
	TableStorage   dp.TableStorageConfig
	NumWorkers     int      `default:"100"`
	BufferSize     int      `default:"500"`
	Ranges         []string `required:"true"`
	RangePrecision int      `default:"3"`
}

// LoadMigrationConfig loads all migration configuration values from env vars.
func LoadMigrationConfig() Config {
	var config Config

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

// Migration contains all objects needed for migration including work queues and worker pools
type Migration struct {
	TableStorage    dp.TableStorageProvider
	Dynamo          dp.DynamoProvider
	Status          dp.DynamoProvider
	ReadWorkQueue   dp.TableStorageReadWork
	ReadWorkerPool  chan dp.TableStorageReadWork
	WriteWorkQueue  dp.DynamoWriteWork
	WriteWorkerPool chan dp.DynamoWriteWork
	Config          Config
	WaitGrp         *sync.WaitGroup
}

// NewMigration returns a migration which has the table storage table, work queue, wait group, etc
func NewMigration(migrationConfig Config) Migration {
	statusProvider := dp.NewMigrationStatusProvider(migrationConfig.Dynamo)
	statusProvider.NewMigrationStatusTable()

	return Migration{
		TableStorage:    dp.NewTableStorageProvider(migrationConfig.TableStorage),
		Dynamo:          dp.NewDynamoProvider(migrationConfig.Dynamo),
		Status:          statusProvider,
		ReadWorkQueue:   make(dp.TableStorageReadWork, migrationConfig.BufferSize),
		ReadWorkerPool:  make(chan dp.TableStorageReadWork, migrationConfig.NumWorkers),
		WriteWorkQueue:  make(dp.DynamoWriteWork, migrationConfig.BufferSize),
		WriteWorkerPool: make(chan dp.DynamoWriteWork, migrationConfig.NumWorkers),
		Config:          migrationConfig,
		WaitGrp:         new(sync.WaitGroup),
	}
}

func queryRangeHasBeenMigrated(alreadyMigrated []dp.QueryRange, queryRange dp.QueryRange) bool {
	for _, value := range alreadyMigrated {
		if value.Ge == queryRange.Ge && value.Lt == queryRange.Lt {
			return true
		}
	}
	return false
}

func (migration *Migration) generateRanges(ranges chan string, currentPrecision int) {
	if currentPrecision == migration.Config.RangePrecision {
		ranges <- migration.Config.Ranges[len(migration.Config.Ranges)-1]
		return
	}

	if len(ranges) == 0 {
		for _, hexCode := range migration.Config.Ranges[0 : len(migration.Config.Ranges)-1] {
			ranges <- hexCode
		}
	} else {
		for i := len(ranges); i > 0; i-- {
			elem := <-ranges
			for _, code := range hexCodes {
				ranges <- elem + code
			}
		}
	}

	currentPrecision++
	migration.generateRanges(ranges, currentPrecision)
}

func (migration *Migration) dispatchReadWork(alreadyMigrated []dp.QueryRange) {
	ranges := make(chan string, 150000)
	migration.generateRanges(ranges, 0)

	close(ranges)
	ge := <-ranges
	queryRange := dp.QueryRange{Ge: ge}

	for lt := range ranges {
		queryRange.Lt = lt

		if !queryRangeHasBeenMigrated(alreadyMigrated, queryRange) {
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
	migration.dispatchReadWork(alreadyMigrated)

	// Wait for work to be completed
	migration.WaitGrp.Wait()
}

//Undo deletes data from table storage in dynamo, or in other words, undoes the migration.
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
	migration.dispatchReadWork([]dp.QueryRange{})

	// Wait for work to be completed
	migration.WaitGrp.Wait()
}

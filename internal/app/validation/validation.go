package validation

// import (
// 	"sync"

// 	"github.com/Azure/azure-sdk-for-go/storage"
// 	"github.com/tablestorage-to-dynamo/internal/app/migration"
// 	dp "github.com/tablestorage-to-dynamo/internal/pkg/dataprovider"
// )

// type Validation struct {
// 	TableStorage            dp.TableStorageProvider
// 	Dynamo                  dp.DynamoProvider
// 	TableStorageWorkQueue   chan dp.DateRange
// 	TableStorageWorkerQueue chan chan dp.DateRange
// 	DynamoWorkQueue         chan []*storage.Entity
// 	DynamoWorkerQueue       chan chan []*storage.Entity
// 	Config                  migration.MigrationConfig
// 	WaitGrp                 *sync.WaitGroup
// }

// // NewValidation returns a migration which has the table storage table, work queue, wait group, etc
// func NewValidation(migrationConfig migration.MigrationConfig) Validation {

// 	return Validation{
// 		TableStorage:            dp.NewTableStorageProvider(migrationConfig.TableStorage),
// 		Dynamo:                  dp.NewDynamoProvier(migrationConfig.Dynamo),
// 		TableStorageWorkQueue:   make(chan dp.DateRange, migrationConfig.BufferSize),
// 		TableStorageWorkerQueue: make(chan chan dp.DateRange, migrationConfig.NumWorkers),
// 		DynamoWorkQueue:         make(chan []*storage.Entity, migrationConfig.BufferSize),
// 		DynamoWorkerQueue:       make(chan chan []*storage.Entity, migrationConfig.NumWorkers),
// 		Config:                  migrationConfig,
// 		WaitGrp:                 new(sync.WaitGroup),
// 	}
// }

// // Migrate migrates data from table storage to dynamo
// func (validation *Validation) Validate() {

// 	// create and start workers
// 	for i := 0; i < migration.Config.NumWorkers; i++ {
// 		readWorker := NewTableStorageReadWorker(i+1, migration.ReadWorkerQueue)
// 		readWorker.Start(migration)

// 		writeWorker := NewWriteWorker(i+1, migration.WriteWorkerQueue)
// 		writeWorker.Start(migration)
// 	}

// 	go func() {
// 		for {
// 			select {
// 			case readWork := <-migration.ReadWorkQueue:
// 				go func() {
// 					worker := <-migration.ReadWorkerQueue
// 					worker <- readWork
// 				}()
// 			case writeWork := <-migration.WriteWorkQueue:
// 				go func() {
// 					worker := <-migration.WriteWorkerQueue
// 					worker <- writeWork
// 				}()
// 			}
// 		}
// 	}()

// 	// generate work
// 	for i := migration.Config.StartDateOffset; i > 0; i-- {
// 		validation.WaitGrp.Add(1)
// 		migration.ReadWorkQueue <- dp.NewDateRange(i, 1)
// 	}

// 	validation.WaitGrp.Wait()
// }

package dataprovider

import (
	"fmt"
	"log"
	"sync"
)

type TableStorageReadWork chan DateRange

type TableStorageReadWorker struct {
	ID         int
	Work       TableStorageReadWork
	WorkerPool chan TableStorageReadWork
	QuitChan   chan bool
}

func NewTableStorageReadWorker(id int, workerPool chan TableStorageReadWork) TableStorageReadWorker {
	worker := TableStorageReadWorker{
		ID:         id,
		Work:       make(TableStorageReadWork),
		WorkerPool: workerPool,
		QuitChan:   make(chan bool),
	}
	return worker
}

func (worker *TableStorageReadWorker) Start(tableStorage *TableStorageProvider, workQueue DynamoWriteWork, wg *sync.WaitGroup) {
	go func() {
		for {
			worker.WorkerPool <- worker.Work
			select {
			case dateRange := <-worker.Work:
				entities := tableStorage.ReadDateRange(dateRange)
				if len(entities) > 0 {
					log.Printf("Read %v from table storage on date range from: %v to: %v. Adding entities to work queue.\n", len(entities), dateRange.FromDate, dateRange.ToDate)
					workQueue <- entities
				} else {
					wg.Done()
				}
			case <-worker.QuitChan:
				fmt.Printf("worker%d: Stopping.", worker.ID)
				return
			}
		}
	}()
}

func (worker *TableStorageReadWorker) Stop() {
	go func() {
		worker.QuitChan <- true
	}()
}

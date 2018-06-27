package dataprovider

import (
	"fmt"
	"log"
	"sync"
)

type TableStorageReadWork chan QueryRange

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

func (worker *TableStorageReadWorker) Start(tableStorage *TableStorageProvider, status *DynamoProvider, workQueue DynamoWriteWork, wg *sync.WaitGroup) {
	go func() {
		for {
			worker.WorkerPool <- worker.Work
			select {
			case queryRange := <-worker.Work:
				log.Printf("Read worker %v: Recieved read work request on range ge: %v and lt: %v\n", worker.ID, queryRange.Ge, queryRange.Lt)
				entities, err := tableStorage.ReadRange(queryRange)

				if err != nil {
					break
				}

				if len(entities) > 0 {
					log.Printf("Read worker %v: Adding %v entities to work queue.\n", worker.ID, len(entities))
					workQueue <- DynamoWriteBatch{queryRange: queryRange, entities: entities}
				} else {
					status.WriteQueryRangeSuccess(queryRange)
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

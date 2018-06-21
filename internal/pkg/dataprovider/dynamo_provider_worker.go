package dataprovider

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoReadWork chan []map[string]*dynamodb.AttributeValue

type DynamoReadWorker struct {
	ID         int
	Work       DynamoReadWork
	WorkerPool chan DynamoReadWork
	QuitChan   chan bool
}

func NewDynamoReadWorker(id int, workerPool chan DynamoReadWork) DynamoReadWorker {
	worker := DynamoReadWorker{
		ID:         id,
		Work:       make(chan []map[string]*dynamodb.AttributeValue),
		WorkerPool: workerPool,
		QuitChan:   make(chan bool),
	}
	return worker
}

// func Start()

func (worker *DynamoReadWorker) Stop() {
	go func() {
		worker.QuitChan <- true
	}()
}

type DynamoWriteBatch struct {
	queryRange QueryRange
	entities   []*storage.Entity
}

type DynamoWriteWork chan DynamoWriteBatch

type DynamoWriteWorker struct {
	ID         int
	Work       DynamoWriteWork
	WorkerPool chan DynamoWriteWork
	QuitChan   chan bool
}

func NewDynamoWriteWorker(id int, workerPool chan DynamoWriteWork) DynamoWriteWorker {
	worker := DynamoWriteWorker{
		ID:         id,
		Work:       make(DynamoWriteWork),
		WorkerPool: workerPool,
		QuitChan:   make(chan bool),
	}
	return worker
}

func StorageEntityToDynamoKey(entity *storage.Entity) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"PartitionKey": {S: aws.String(entity.PartitionKey)},
		"RowKey":       {S: aws.String(entity.RowKey)},
	}
}

func StorageEntityToDynamoMap(entity *storage.Entity, columnNames *[]string) map[string]*dynamodb.AttributeValue {
	dynamoMap := map[string]*dynamodb.AttributeValue{
		"PartitionKey": {S: aws.String(entity.PartitionKey)},
		"RowKey":       {S: aws.String(entity.RowKey)},
		"Timestamp":    {S: aws.String(entity.TimeStamp.UTC().Format("2006-01-02T15:04:05.999999Z"))},
	}

	for _, key := range *columnNames {
		switch value := entity.Properties[key].(type) {
		case string:
			if value != "" {
				dynamoMap[key] = &dynamodb.AttributeValue{S: aws.String(value)}
			}
		case int32:
			dynamoMap[key] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(int64(value), 10))}
		case int64:
			dynamoMap[key] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(value, 10))}
		case float64:
			dynamoMap[key] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatFloat(value, 'f', -1, 64))}
		case bool:
			dynamoMap[key] = &dynamodb.AttributeValue{BOOL: aws.Bool(value)}
		case time.Time:
			dynamoMap[key] = &dynamodb.AttributeValue{S: aws.String(value.UTC().Format("2006-01-02T15:04:05.999999Z"))}
		}
	}

	return dynamoMap
}

func (worker *DynamoWriteWorker) Start(dynamo *DynamoProvider, status *DynamoProvider, columnNames *[]string, wg *sync.WaitGroup) {
	go func() {
		for {
			worker.WorkerPool <- worker.Work
			select {
			case writeBatch := <-worker.Work:
				log.Printf("Write worker %v: Recieved write work request for %v entities\n", worker.ID, len(writeBatch.entities))

				dynamoMapList := make([]map[string]*dynamodb.AttributeValue, len(writeBatch.entities))
				for i, entity := range writeBatch.entities {
					dynamoMapList[i] = StorageEntityToDynamoMap(entity, columnNames)
				}

				dynamo.WriteToDynamo(dynamoMapList, GetDynamoPutRequests)
				status.WriteQueryRangeSuccess(writeBatch.queryRange)

				log.Printf("Write worker %v: Finished write work request for %v entities\n", worker.ID, len(writeBatch.entities))
				wg.Done()
			case <-worker.QuitChan:
				fmt.Printf("worker%d: Stopping.", worker.ID)
				return
			}
		}
	}()
}

func (worker *DynamoWriteWorker) StartDelete(dynamo *DynamoProvider, wg *sync.WaitGroup) {
	go func() {
		for {
			worker.WorkerPool <- worker.Work
			select {
			case writeBatch := <-worker.Work:
				log.Printf("Write worker %v: Recieved delete work request for %v entities\n", worker.ID, len(writeBatch.entities))

				dynamoMapList := make([]map[string]*dynamodb.AttributeValue, len(writeBatch.entities))
				for i, entity := range writeBatch.entities {
					dynamoMapList[i] = StorageEntityToDynamoKey(entity)
				}

				dynamo.WriteToDynamo(dynamoMapList, GetDynamoDeleteRequests)

				log.Printf("Write worker %v: Finished delete work request for %v entities\n", worker.ID, len(writeBatch.entities))
				wg.Done()
			case <-worker.QuitChan:
				fmt.Printf("worker%d: Stopping.", worker.ID)
				return
			}
		}
	}()
}

func (worker *DynamoWriteWorker) Stop() {
	go func() {
		worker.QuitChan <- true
	}()
}

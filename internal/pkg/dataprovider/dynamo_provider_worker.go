package dataprovider

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type dynamoReadWork chan []map[string]*dynamodb.AttributeValue

// DynamoReadWorker worker consists of work and shared worker pool
type DynamoReadWorker struct {
	ID         int
	Work       dynamoReadWork
	WorkerPool chan dynamoReadWork
	QuitChan   chan bool
}

// NewDynamoReadWorker given shared worker pool creates a new dynamo read worker that will be added to worker pool.
func NewDynamoReadWorker(id int, workerPool chan dynamoReadWork) DynamoReadWorker {
	worker := DynamoReadWorker{
		ID:         id,
		Work:       make(chan []map[string]*dynamodb.AttributeValue),
		WorkerPool: workerPool,
		QuitChan:   make(chan bool),
	}
	return worker
}

// Stop halts worker
func (worker *DynamoReadWorker) Stop() {
	go func() {
		worker.QuitChan <- true
	}()
}

// DynamoWriteBatch represents a query range and corresponding entries in that range
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

func storageEntityToDynamoKey(entity *storage.Entity) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"PartitionKey": {S: aws.String(entity.PartitionKey)},
		"RowKey":       {S: aws.String(entity.RowKey)},
	}
}

// what product to set? 1 - just set ILL; 2 - use activity list to decide right products; 3 - add "unknown" product; 4 - just leave blank

func storageEntityToDynamoMap(entity *storage.Entity, columnNames *[]string) map[string]*dynamodb.AttributeValue {
	attributeValue := 0

	if entity.Properties["ArtifactType"].(string) == "Writing" {
		attributeValue = 1
	}

	return map[string]*dynamodb.AttributeValue{
		"ArtifactId":         &dynamodb.AttributeValue{S: aws.String(entity.RowKey)},
		"ArtifactType":       &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(int64(attributeValue), 10))}, // fix the type here
		"Created":            &dynamodb.AttributeValue{S: aws.String(entity.TimeStamp.UTC().Format("2006-01-02T15:04:05.999999Z"))},
		"InternalProductKey": &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("%s/%s", entity.Properties["Activity"], entity.Properties["Dataset"]))},
		"IsDeleting":         &dynamodb.AttributeValue{BOOL: aws.Bool(false)},
		"Product":            &dynamodb.AttributeValue{N: aws.String("0")}, // All are lang and lit here
		"StudentId":          &dynamodb.AttributeValue{S: aws.String(entity.PartitionKey)},
		"Title":              &dynamodb.AttributeValue{S: aws.String(entity.Properties["Title"].(string))},
		"Url":                &dynamodb.AttributeValue{S: aws.String(strings.Replace(entity.Properties["Url"].(string), ".", "/", -1))},
		"Description":        &dynamodb.AttributeValue{S: aws.String(entity.Properties["Description"].(string))},
		"Review":             &dynamodb.AttributeValue{BOOL: aws.Bool(entity.Properties["Review"].(bool))},
		"Text":               &dynamodb.AttributeValue{S: aws.String(entity.Properties["Text"].(string))},
		"Updated":            &dynamodb.AttributeValue{S: aws.String(entity.TimeStamp.UTC().Format("2006-01-02T15:04:05.999999Z"))},
	}
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
					dynamoMapList[i] = storageEntityToDynamoMap(entity, columnNames)
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
					dynamoMapList[i] = storageEntityToDynamoKey(entity)
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

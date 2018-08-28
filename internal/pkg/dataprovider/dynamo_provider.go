package dataprovider

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// GetWriteRequests typecast func for building delete and write request structs
type GetWriteRequests func(input []map[string]*dynamodb.AttributeValue) []*dynamodb.WriteRequest

// DynamoConfig config data for dynamo connection and status table name
type DynamoConfig struct {
	Region                   string `default:"us-west-2"`
	TableName                string `required:"true"`
	MigrationStatusTableName string `require:"true"`
}

var (
	batchWriteSize = 25
	batchReadSize  = 100
)

// DynamoProvider contains service for all dynamo calls and table name
type DynamoProvider struct {
	Service   *dynamodb.DynamoDB
	TableName string
}

// NewDynamoProvider connects to a dynamo service provider and returns new DynamoProvider struct
func NewDynamoProvider(config DynamoConfig) DynamoProvider {
	return DynamoProvider{
		Service: dynamodb.New(session.New(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewEnvCredentials(),
		})),
		TableName: config.TableName,
	}
}

// NewMigrationStatusProvider connects to a dynamo service provider using the migration status table name
func NewMigrationStatusProvider(config DynamoConfig) DynamoProvider {
	return DynamoProvider{
		Service: dynamodb.New(session.New(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewEnvCredentials(),
		})),
		TableName: config.MigrationStatusTableName,
	}
}

func (dynamoProvider *DynamoProvider) waitForTableToBeReady() error {
	maxWaitSeconds := 60
	currentWaitSeconds := 0
	ticker := time.Tick(1 * time.Second)

	for range ticker {
		currentWaitSeconds++
		if currentWaitSeconds > maxWaitSeconds {
			return errors.New("table did not become ready within 1 minute")
		}

		describeTableInput := &dynamodb.DescribeTableInput{
			TableName: &dynamoProvider.TableName,
		}

		response, err := dynamoProvider.Service.DescribeTable(describeTableInput)

		if err != nil {
			return err
		}

		if response.Table == nil {
			continue
		}

		status := *response.Table.TableStatus
		if status == dynamodb.TableStatusActive {
			return nil
		}
	}

	return nil
}

// NewMigrationStatusTable creates a new status table to store the state of the migration (i.e successfully migrated ranges)
func (dynamoProvider *DynamoProvider) NewMigrationStatusTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Ge"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("Lt"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Ge"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("Lt"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(100),
			WriteCapacityUnits: aws.Int64(100),
		},
		TableName: &dynamoProvider.TableName,
	}

	_, err := dynamoProvider.Service.CreateTable(input)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeTableAlreadyExistsException:
				log.Println("Status table already exists from previous migration. Using status data.")
			case dynamodb.ErrCodeResourceInUseException:
				log.Println("Status table already exists from previous migration. Using status data.")
			default:
				log.Printf("Got error calling CreateTable: %v", err)
			}
		} else {
			log.Println(err.Error())
		}
		return err
	}

	log.Printf("Initializing status table.")
	dynamoProvider.waitForTableToBeReady()
	log.Printf("Created table %v", dynamoProvider.TableName)

	return nil
}

// DeleteMigrationStatusTable helper function to clean up migration status table.
func (dynamoProvider *DynamoProvider) DeleteMigrationStatusTable() error {
	input := &dynamodb.DeleteTableInput{
		TableName: &dynamoProvider.TableName,
	}

	_, err := dynamoProvider.Service.DeleteTable(input)

	if err != nil {
		log.Printf("Error deleting migration status table: %v", err)
		return err
	}

	log.Printf("Deleted table %v", dynamoProvider.TableName)
	return nil
}

// ScanStatusTable reads all ranges from status table
func (dynamoProvider *DynamoProvider) ScanStatusTable() []QueryRange {
	migrationStatus := []QueryRange{}

	input := &dynamodb.ScanInput{
		TableName: &dynamoProvider.TableName,
	}

	response, err := dynamoProvider.Service.Scan(input)

	if err != nil {
		log.Printf("Cannot read entries from status table: %v", err)
	}

	err = dynamodbattribute.UnmarshalListOfMaps(response.Items, &migrationStatus)
	if err != nil {
		log.Printf("failed to unmarshal Dynamodb Scan Items, %v", err)
	}

	for response.LastEvaluatedKey != nil {
		input = &dynamodb.ScanInput{
			TableName:         &dynamoProvider.TableName,
			ExclusiveStartKey: response.LastEvaluatedKey,
		}

		response, err = dynamoProvider.Service.Scan(input)

		if err != nil {
			log.Printf("Cannot read entries from status table: %v", err)
		}

		nextPage := []QueryRange{}
		err = dynamodbattribute.UnmarshalListOfMaps(response.Items, &nextPage)
		if err != nil {
			log.Printf("failed to unmarshal Dynamodb Scan Items, %v", err)
		}
		migrationStatus = append(migrationStatus, nextPage...)
	}

	return migrationStatus
}

// ScanTable reads all entries from table and returns a list of map[string]*AttributeValue
func (dynamoProvider *DynamoProvider) ScanTable() []map[string]*dynamodb.AttributeValue {
	input := &dynamodb.ScanInput{
		TableName: &dynamoProvider.TableName,
	}

	response, err := dynamoProvider.Service.Scan(input)

	if err != nil {
		log.Printf("Cannot read entries from status table: %v", err)
	}

	return response.Items
}

// PutItem puts a single item into dynamo table
func (dynamoProvider *DynamoProvider) PutItem(item map[string]*dynamodb.AttributeValue) {
	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: &dynamoProvider.TableName,
	}

	_, err := dynamoProvider.Service.PutItem(input)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				log.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				log.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				log.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				log.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				log.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return
	}
}

// WriteQueryRangeSuccess upon successful migration of query ranges writes this range to the migration status table.
func (dynamoProvider *DynamoProvider) WriteQueryRangeSuccess(queryRange QueryRange) {
	item := map[string]*dynamodb.AttributeValue{
		"Ge": {S: aws.String(queryRange.Ge)},
		"Lt": {S: aws.String(queryRange.Lt)},
	}
	dynamoProvider.PutItem(item)
}

// BatchWrite writes a batch to dynamo. Batches are 25 entries
func (dynamoProvider *DynamoProvider) BatchWrite(input map[string][]*dynamodb.WriteRequest) {
	writeInput := &dynamodb.BatchWriteItemInput{RequestItems: input}
	result, err := dynamoProvider.Service.BatchWriteItem(writeInput)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				log.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				log.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				log.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				log.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
	}
	if len(result.UnprocessedItems) > 0 {
		dynamoProvider.BatchWrite(result.UnprocessedItems)
	}
}

func GetDynamoPutRequests(input []map[string]*dynamodb.AttributeValue) []*dynamodb.WriteRequest {
	writeRequests := []*dynamodb.WriteRequest{}

	for _, attrValue := range input {
		writeRequest := dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: attrValue,
			},
		}
		writeRequests = append(writeRequests, &writeRequest)
	}

	return writeRequests
}

func GetDynamoDeleteRequests(input []map[string]*dynamodb.AttributeValue) []*dynamodb.WriteRequest {
	writeRequests := []*dynamodb.WriteRequest{}

	for _, attrValue := range input {
		writeRequest := dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: attrValue,
			},
		}
		writeRequests = append(writeRequests, &writeRequest)
	}

	return writeRequests
}

func (dynamoProvider *DynamoProvider) WriteToDynamo(input []map[string]*dynamodb.AttributeValue, fn GetWriteRequests) {
	var wg sync.WaitGroup
	for i := 0; i < len(input); i += batchWriteSize {
		wg.Add(1)
		go func(start int) {
			end := start + batchWriteSize
			if end > len(input) {
				end = len(input)
			}
			writeRequestItems := map[string][]*dynamodb.WriteRequest{
				dynamoProvider.TableName: fn(input[start:end]),
			}
			dynamoProvider.BatchWrite(writeRequestItems)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

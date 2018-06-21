package dataprovider

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type DynamoOperation func(input []map[string]*dynamodb.AttributeValue, wg *sync.WaitGroup)
type GetWriteRequests func(input []map[string]*dynamodb.AttributeValue) []*dynamodb.WriteRequest

type DynamoConfig struct {
	Region    string `default:"us-west-2"`
	TableName string `required:"true"`
}

type DynamoProvider struct {
	Service   *dynamodb.DynamoDB
	TableName string
}

var (
	BatchWriteSize  int = 25
	BatchReadSize   int = 100
	DateRangeStatus map[QueryRange]*int32
)

func NewDynamoProvier(config DynamoConfig) DynamoProvider {
	return DynamoProvider{
		Service: dynamodb.New(session.New(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewEnvCredentials(),
		})),
		TableName: config.TableName,
	}
}

func (dynamoProvider *DynamoProvider) NewMigrationStatusTable() {
	tableName := "TsToDynamoMigrationStatus"
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
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(tableName),
	}

	_, err := dynamoProvider.Service.CreateTable(input)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeTableAlreadyExistsException:
				log.Println("Status table already exists for migration. Using data from migration status table.")
			case dynamodb.ErrCodeResourceInUseException:
				log.Println("Status table already exists for migration. Using data from migration status table.")
			default:
				log.Println("Got error calling CreateTable:")
				log.Println(err.Error())
				os.Exit(1)
			}
		} else {
			log.Println(err.Error())
		}
	} else {
		log.Printf("Created table %v", tableName)
	}

	dynamoProvider.TableName = tableName
}

func (dynamoProvider *DynamoProvider) DeleteMigrationStatusTable() {
	input := &dynamodb.DeleteTableInput{
		TableName: &dynamoProvider.TableName,
	}

	_, err := dynamoProvider.Service.DeleteTable(input)

	if err != nil {
		log.Printf("Error deleting migration status table: %v", err)
	}
}

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
		panic(fmt.Sprintf("failed to unmarshal Dynamodb Scan Items, %v", err))
	}

	return migrationStatus
}

func (dynamoProvider *DynamoProvider) PutItem(item map[string]*dynamodb.AttributeValue) {
	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(dynamoProvider.TableName),
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

func (dynamoProvider *DynamoProvider) WriteQueryRangeSuccess(queryRange QueryRange) {
	item := map[string]*dynamodb.AttributeValue{
		"Ge": {S: aws.String(queryRange.Ge)},
		"Lt": {S: aws.String(queryRange.Lt)},
	}
	dynamoProvider.PutItem(item)
}

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
	for i := 0; i < len(input); i += BatchWriteSize {
		wg.Add(1)
		go func(start int) {
			end := start + BatchWriteSize
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

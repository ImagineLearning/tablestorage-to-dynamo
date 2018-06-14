package dataprovider

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
	BatchWriteSize int = 25
	BatchReadSize  int = 100
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

func (dynamoProvider *DynamoProvider) BatchWrite(input map[string][]*dynamodb.WriteRequest) {
	writeInput := &dynamodb.BatchWriteItemInput{RequestItems: input}
	result, err := dynamoProvider.Service.BatchWriteItem(writeInput)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
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

package dataprovider

import (
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/storage"
)

type TableStorageConfig struct {
	AccountName string   `required:"true"`
	AccountKey  string   `required:"true"`
	TableName   string   `required:"true"`
	ColumnNames []string `required:"true"` // an array of column names other than partition key, row key, and timestamp
}

type TableStorageProvider struct {
	Table *storage.Table
}

type QueryRange struct {
	Ge string
	Lt string
}

// InitTableStorageProvider connects to table storage and dynamo tables
func NewTableStorageProvider(config TableStorageConfig) TableStorageProvider {
	cli, err := storage.NewBasicClient(config.AccountName, config.AccountKey)

	if err != nil {
		log.Fatal(err)
	}

	tableService := cli.GetTableService()

	return TableStorageProvider{
		Table: tableService.GetTableReference(config.TableName),
	}
}

// ReadRange queries table storage on a range and returns the response
func (provider *TableStorageProvider) ReadRange(queryRange QueryRange) ([]*storage.Entity, error) {
	results := []*storage.Entity{}
	filter := fmt.Sprintf("PartitionKey ge '%v' and PartitionKey lt '%v'", queryRange.Ge, queryRange.Lt)
	options := storage.QueryOptions{
		Filter: filter,
	}

	result, err := provider.Table.QueryEntities(30, storage.FullMetadata, &options)
	if err != nil {
		log.Printf("Error reading range from table storage: %v", err)
		return nil, err
	}

	results = append(results, result.Entities...)

	for result.NextLink != nil {
		result, err = result.NextResults(nil)
		if err != nil {
			log.Printf("Error reading next page from table storage: %v", err)
			return nil, err
		}

		results = append(results, result.Entities...)
	}
	return results, nil
}

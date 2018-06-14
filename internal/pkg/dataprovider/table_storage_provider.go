package dataprovider

import (
	"fmt"
	"log"
	"time"

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

type DateRange struct {
	FromDate string
	ToDate   string
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

// NewDateRange returns a UTC string representation of a date range
// For example, NewDateRange(1, 1) will return the rang of yesterday with a 1 day span (yesterday to today)
func NewDateRange(fromOffset int, dateRange int) DateRange {
	from := (fromOffset * dateRange)
	to := (fromOffset * dateRange) - dateRange
	return DateRange{
		FromDate: time.Now().AddDate(0, 0, -from).UTC().Format("2006-01-02T15:04:05.999999Z"),
		ToDate:   time.Now().AddDate(0, 0, -to).UTC().Format("2006-01-02T15:04:05.999999Z"),
	}
}

// ReadDateRange queries table storage on a date range and fills up the work queue
// TODO: Error handling.
func (provider *TableStorageProvider) ReadDateRange(dateRange DateRange) []*storage.Entity {
	results := []*storage.Entity{}
	filter := fmt.Sprintf("Timestamp ge datetime'%v' and Timestamp le datetime'%v'", dateRange.FromDate, dateRange.ToDate)

	options := storage.QueryOptions{
		Filter: filter,
	}

	result, err := provider.Table.QueryEntities(30, storage.FullMetadata, &options)

	if err != nil {
		fmt.Println(err)
	}

	results = append(results, result.Entities...)
	for result.NextLink != nil {
		result, err = result.NextResults(nil)

		if err != nil {
			fmt.Println(err)
		}

		results = append(results, result.Entities...)
	}
	return results
}

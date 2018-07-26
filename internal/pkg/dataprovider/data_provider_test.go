package dataprovider

import (
	"testing"
)

func TestNewTableStorageProvider(t *testing.T) {
	provider := NewTableStorageProvider(Config.TableStorage)

	if provider.Table == nil {
		t.Errorf("Could not initialize table storage provider.")
	}
}

func TestNewQueryRange(t *testing.T) {
	queryRange := NewQueryRange("00", "ff")

	if queryRange.Ge != "00" || queryRange.Lt != "ff" {
		t.Errorf("Could not get date range.")
	}
}

func TestReadFromTableStorage(t *testing.T) {

	provider := NewTableStorageProvider(Config.TableStorage)
	results, err := provider.ReadRange(NewQueryRange("00", "0f"))

	if len(results) == 0 || err != nil {
		t.Errorf("Could not read from table storage: %v", err)
	}
}

func TestNewDynamoProvider(t *testing.T) {
	provider := NewDynamoProvider(Config.Dynamo)

	if provider.TableName == "" || provider.Service == nil {
		t.Errorf("Could not initialize dynamo provider.")
	}
}

func TestMigrationStatusTable(t *testing.T) {
	newConfig := Config.Dynamo
	newConfig.MigrationStatusTableName = "testMigrationStatusTable"
	provider := NewMigrationStatusProvider(newConfig)
	err := provider.NewMigrationStatusTable()

	if err != nil {
		t.Errorf("Could not create migration status table: %v", err)
	}

	err = provider.DeleteMigrationStatusTable()

	if err != nil {
		t.Errorf("Could not delete migration status table: %v", err)
	}
}

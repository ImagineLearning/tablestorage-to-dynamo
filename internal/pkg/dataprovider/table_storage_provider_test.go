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

func TestNewDateRange(t *testing.T) {
	dateRange := NewDateRange(1, 1)

	if dateRange.FromDate == "" || dateRange.ToDate == "" {
		t.Errorf("Could not get date range.")
	}
}

func TestReadFromTableStorage(t *testing.T) {

	provider := NewTableStorageProvider(Config.TableStorage)
	entities := provider.ReadDateRange(NewDateRange(1, 1))

	if len(entities) == 0 {
		t.Errorf("Data could not be read from table storage.")
	}
}

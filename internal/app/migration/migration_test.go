package migration

import (
	"testing"
)

func TestMigrate(t *testing.T) {
	migration := NewMigration(Config)
	migration.Start()

	results := migration.Dynamo.ScanTable()

	if len(results) == 0 {
		t.Errorf("TestMigrate failed. No data has been migrated.")
	}
}

func TestUndo(t *testing.T) {
	migration := NewMigration(Config)
	migration.Undo()

	results := migration.Dynamo.ScanTable()

	if len(results) != 0 {
		t.Errorf("TestUndo failed. Data still exists in table.")
	}
}

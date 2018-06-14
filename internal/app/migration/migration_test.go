package migration

import (
	"testing"
)

func TestMigrate(t *testing.T) {
	migration := NewMigration(Config)
	migration.Start()
}

func TestUnMigrate(t *testing.T) {
	migration := NewMigration(Config)
	migration.Undo()
}

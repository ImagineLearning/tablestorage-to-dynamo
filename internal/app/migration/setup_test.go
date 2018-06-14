package migration

import (
	"os"
	"testing"
)

var (
	Config MigrationConfig
)

func TestMain(m *testing.M) {
	Config = LoadMigrationConfig()
	retCode := m.Run()
	os.Exit(retCode)
}

package validation

import (
	"os"
	"testing"
)

var (
	Config MigrationConfig
)

func TestMain(m *testing.M) {
	Config = NewMigrationConfigFromFile("../../../configs/config.json")
	retCode := m.Run()
	os.Exit(retCode)
}

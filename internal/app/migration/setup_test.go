package migration

import (
	"os"
	"testing"
)

var (
	config Config
)

func TestMain(m *testing.M) {
	config = LoadMigrationConfig()
	retCode := m.Run()
	os.Exit(retCode)
}

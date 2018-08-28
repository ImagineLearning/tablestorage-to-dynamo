package dataprovider

import (
	"log"
	"os"
	"testing"

	"github.com/kelseyhightower/envconfig"
)

type TestingConfig struct {
	Dynamo       DynamoConfig       `json:"dynamo"`
	TableStorage TableStorageConfig `json:"tableStorage"`
}

var (
	config TestingConfig
)

func LoadTestingConfig() TestingConfig {

	var testingConfig TestingConfig
	var dynamoConfig DynamoConfig
	err := envconfig.Process("DYNAMO", &dynamoConfig)

	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	testingConfig.Dynamo = dynamoConfig

	var tsConfig TableStorageConfig
	err = envconfig.Process("TABLESTORAGE", &tsConfig)

	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	testingConfig.TableStorage = tsConfig

	return testingConfig
}

func TestMain(m *testing.M) {
	config = LoadTestingConfig()
	retCode := m.Run()
	os.Exit(retCode)
}

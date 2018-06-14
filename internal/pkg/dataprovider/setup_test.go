package dataprovider

import (
	"fmt"
	"os"
	"testing"

	"github.com/kelseyhightower/envconfig"
)

type TestingConfig struct {
	Dynamo       DynamoConfig       `json:"dynamo"`
	TableStorage TableStorageConfig `json:"tableStorage"`
}

var (
	Config TestingConfig
)

func LoadTestingConfig() TestingConfig {

	var testingConfig TestingConfig
	var dynamoConfig DynamoConfig
	err := envconfig.Process("DYNAMO", &dynamoConfig)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	testingConfig.Dynamo = dynamoConfig

	var tsConfig TableStorageConfig
	err = envconfig.Process("TABLESTORAGE", &tsConfig)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	testingConfig.TableStorage = tsConfig

	return testingConfig
}

func TestMain(m *testing.M) {
	Config = LoadTestingConfig()
	retCode := m.Run()
	os.Exit(retCode)
}

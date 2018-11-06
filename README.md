# tablestorage-to-dynamo
Script written in go to migrate from table storage to dynamo db.

### Introduction

This is a highly performant and failure tolerant solution for migrating data from azure table storage to dynamo db. It uses a worker pool, work queue, dispatcher pattern.

## Setup
To run this migration script you need to configure the following env variables: 
```
    "DYNAMO_TABLENAME": "TargetTableName",
    "DYNAMO_REGION": "us-west-2",
    "DYNAMO_MIGRATIONSTATUSTABLENAME": "MigrationStatusTableName",
    "TABLESTORAGE_ACCOUNTNAME": "TableStorageAccountName",
    "TABLESTORAGE_ACCOUNTKEY": "TableStorageAccountKey",
    "TABLESTORAGE_TABLENAME": "TableStorageTableName",
    "TABLESTORAGE_COLUMNNAMES": "Column1,Column2,Column3",
    "AWS_ACCESS_KEY_ID": "AWSAccessKeyId",
    "AWS_SECRET_ACCESS_KEY": "AWSSecretAccessKey",
    "NUMWORKERS": 20,
    "BUFFERSIZE": 500,
    "RANGES": "0,1,2,3,4",
    "RANGEPRECISION": "3",
```

* Dynamo_MigrationStatusTableName - Name of nonexistent migration status table. Will be created at the start of migration.
* TableStorage_ColumnNames - A comma delimited list (no spaces unless in column name) of columns to migrate
* Ranges - Hexadecimal ranges, comma separated, no spaces. For example, "0,2,4,6,8,a,c,e,g" where g is not inclusive. 

## Job Config
This script was used to migrate 110 million entries in ~8 hours. One way to facilitate such a large migration is to use kubernetes jobs (we already had a kubernetes cluster so this was easy to do). The benefit of using kuberentes jobs was that jobs are automatically restarted when they fail (jobs are bound to fail), and we could further parallelize the migration. The script is written to use a status table that can quickly pick up a migration where it was left off.


## Performance
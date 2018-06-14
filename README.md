# tablestorage-to-dynamo-migration
Script written in go to migrate from table storage to dynamo db.

### Introduction

This is a high performance script to migrate data from azure table storage to aws dynamo db. It uses a worker pool, work queue, dispatcher pattern to optimize reads from table storage and writes to dynamo db. 

## Setup
To run this migration script you need to configure the following env variables: 
```
    "DYNAMO_TABLENAME": "myDynamoTable",
    "DYNAMO_REGION": "us-west-2",
    "TABLESTORAGE_ACCOUNTNAME": "myAccountname",
    "TABLESTORAGE_ACCOUNTKEY": "myAccountKey",
    "TABLESTORAGE_TABLENAME": "myTsTableName",
    "TABLESTORAGE_COLUMNNAMES": "Value",
    "AWS_ACCESS_KEY_ID": "myAWSAccessKeyID",
    "AWS_SECRET_ACCESS_KEY": "myAWSSecretAccessKey",
    "NUMWORKERS": 100, 
    "STARTDATEOFFSET": 100,
    "ENDDATEOFFSET": 0,
    "BUFFERSIZE": 500,
```

```
TABLESTORAGE_COLUMNNAMES - Comma seperated string of all column names other than partition key, row key, and timestamp 
NUMWORKERS - # of workers in worker pool. Best performance w/ # workers = STARTDATEOFFSET - ENDDATEOFFSET
STARTDATEOFFSET - How many days previous to today to begin migrating
ENDDATEOFFSET - How many days previous to today to end migrating. 0 for time.Now()
BUFFERSIZE - Size of work channels. 500 should be sufficient
```

### Job Config



## Performance
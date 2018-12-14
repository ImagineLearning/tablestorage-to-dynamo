FROM golang:1.11 AS builder

# Copy src
ADD . /tablestorage-to-dynamo

WORKDIR /tablestorage-to-dynamo

RUN go build cmd/migration/main.go
CMD ["./main"]
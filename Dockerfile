FROM golang:1.9 AS builder

# Build Directories 
RUN mkdir /go/src/github.com
RUN mkdir /go/src/github.com/tablestorage-to-dynamo-migration

# Copy src and vendors
ADD ./cmd /go/src/github.com/tablestorage-to-dynamo-migration/cmd
ADD ./internal /go/src/github.com/tablestorage-to-dynamo-migration/internal
ADD ./vendor /go/src/github.com/tablestorage-to-dynamo-migration/vendor

WORKDIR /go/src/github.com/tablestorage-to-dynamo-migration

RUN go build cmd/migration/main.go
CMD ["./main"]
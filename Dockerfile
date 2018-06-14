FROM golang:1.9 AS builder

# Build Directories 
RUN mkdir /go/src/github.com
RUN mkdir /go/src/github.com/tablestorage-to-dynamo

# Copy src and vendors
ADD ./cmd /go/src/github.com/tablestorage-to-dynamo/cmd
ADD ./internal /go/src/github.com/tablestorage-to-dynamo/internal
ADD ./vendor /go/src/github.com/tablestorage-to-dynamo/vendor

WORKDIR /go/src/github.com/tablestorage-to-dynamo

RUN go build cmd/migration/main.go
CMD ["./main"]
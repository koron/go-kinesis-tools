package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func main() {
	ss := session.New(
		&aws.Config{
			Region:      aws.String("ap-northeast-1"),
			Credentials: credentials.NewSharedCredentials("", ""),
		})
	svc := kinesis.New(ss)

	resp, err := svc.PutRecord(&kinesis.PutRecordInput{
		StreamName:   aws.String("kaoriya-kinesis-test"),
		PartitionKey: aws.String("cmd/send"),
		Data:         []byte("foobar"),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp)
}

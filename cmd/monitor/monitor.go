package main

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func get(svc *kinesis.Kinesis) {
	params := &kinesis.GetRecordsInput{
		ShardIterator: aws.String("kaoriya-kinesis-test"),
		Limit:         aws.Int64(1),
	}
	resp, err := svc.GetRecords(params)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp)
}

func listStreams(svc *kinesis.Kinesis) {
	resp, err := svc.ListStreams(&kinesis.ListStreamsInput{
		Limit: aws.Int64(10),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp)
}

func shards(svc *kinesis.Kinesis) ([]*kinesis.Shard, error) {
	resp, err := svc.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String("kaoriya-kinesis-test"),
	})
	if err != nil {
		return nil, err
	}
	fmt.Println(resp)
	return resp.StreamDescription.Shards, nil
}

func main() {
	ss := session.New(
		&aws.Config{
			Region:      aws.String("ap-northeast-1"),
			Credentials: credentials.NewSharedCredentials("", ""),
		})
	svc := kinesis.New(ss)

	list, err := shards(svc)
	if err != nil {
		log.Fatal(err)
	}
	if len(list) == 0 {
		log.Fatal("no shards")
	}

	shard := list[0]
	shardIter, err := svc.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        aws.String("kaoriya-kinesis-test"),
		ShardId:           shard.ShardId,
		ShardIteratorType: aws.String("TRIM_HORIZON"),
	})
	if err != nil {
		log.Fatal(err)
	}

	iter := shardIter.ShardIterator
	for {
		recs, err := svc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: iter,
		})
		if err != nil {
			log.Fatalf("GetRecords: %v", err)
		}
		iter = recs.NextShardIterator
		log.Printf("Records: %s", recs.Records)
		if len(recs.Records) == 0 {
			time.Sleep(time.Second * 5)
		}
	}
}

package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
)

func main() {

	bucket := "build33-photos-raw"
	sess := session.New()
	svc := s3.New(sess)

	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	pageNum := 0
	objectNum := 0
	err := svc.ListObjectsPages(
		params,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			pageNum++
			for _, value := range page.Contents {
				objectNum++
				fmt.Println(*value.Key, *value.ETag)
			}
			return true
		})
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", bucket, err)
	}
	fmt.Println(objectNum)
}

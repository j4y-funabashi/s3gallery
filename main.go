package main

import (
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

func main() {

	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		log.Fatalf("Could not connect to db, %v", err)
	}
	createQuery := "CREATE TABLE IF NOT EXISTS ext_photos(id INTEGER PRIMARY KEY AUTOINCREMENT, key VARCHAR(255), file_hash CHAR(35))"
	stmt, err := db.Prepare(createQuery)
	if err != nil {
		log.Fatalf("Could not prepare create table query: %v", err)
	}
	stmt.Exec()
	stmt.Close()

	bucket := "build33-photos-raw"
	sess := session.New()
	svc := s3.New(sess)

	insertQuery := "INSERT INTO ext_photos (file_hash, key) VALUES (?, ?)"
	insertStmt, err := db.Prepare(insertQuery)
	if err != nil {
		log.Fatalf("Could not prepare insert query: %v", err)
	}
	defer insertStmt.Close()
	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	pageNum := 0
	objectNum := 0
	err = svc.ListObjectsPages(
		params,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			pageNum++
			for _, value := range page.Contents {
				insertStmt.Exec(*value.ETag, *value.Key)
				objectNum++
			}
			return true
		})
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", bucket, err)
	}
	fmt.Println(objectNum)
}

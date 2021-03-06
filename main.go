package main

import (
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func main() {
	bucket := "build33-photos-raw"
	importData(bucket)
}

func importData(bucket string) {
	db := initDB()
	createTables(db)

	sess := session.New()
	s3svc := s3.New(sess)
	s3manager := s3manager.NewDownloader(sess)

	db.Exec("BEGIN TRANSACTION")

	insertQuery := "REPLACE INTO ext_photos (file_hash, key) VALUES (?, ?)"
	insertStmt, err := db.Prepare(insertQuery)
	if err != nil {
		log.Fatalf("Could not prepare insert query: %v", err)
	}
	defer insertStmt.Close()
	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	objectCount := 0

	err = s3svc.ListObjectsPages(
		params,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, value := range page.Contents {
				etag := *value.ETag
				key := *value.Key
				if strings.ToLower(key[len(key)-3:len(key)]) != "jpg" {
					continue
				}

				// download file
				file, err := ioutil.TempFile("", "")
				if err != nil {
					log.Fatalf("Could not create temp file: %v", err)
				}
				input := s3.GetObjectInput{Bucket: &bucket, Key: value.Key}
				_, err = s3manager.Download(file, &input)
				if err != nil {
					log.Fatalf("Could not download file: %v", err)
				}
				os.Remove(file.Name())

				// insert to db
				_, err = insertStmt.Exec(etag[1:len(etag)-1], *value.Key)
				if err != nil {
					log.Fatalf("Could not insert: %v", err)
				}
				objectCount++
			}
			return true
		})
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", bucket, err)
	}

	db.Exec("END TRANSACTION")

	fmt.Println(objectCount)
}

func initDB() *sql.DB {
	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		log.Fatalf("Could not connect to db, %v", err)
	}
	db.Exec("PRAGMA synchronous = OFF")
	return db
}

func createTables(db *sql.DB) {
	createQuery := "CREATE TABLE IF NOT EXISTS ext_photos(id INTEGER PRIMARY KEY AUTOINCREMENT, key VARCHAR(255) UNIQUE, file_hash CHAR(35))"
	stmt, err := db.Prepare(createQuery)
	if err != nil {
		log.Fatalf("Could not prepare create table query: %v", err)
	}
	stmt.Exec()
	stmt.Close()
}

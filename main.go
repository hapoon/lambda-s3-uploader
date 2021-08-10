package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	region         string
	downloadBucket string
	uploadBucket   string
)

func main() {
	lambda.Start(HandleRequest)
}

type LambdaEvent struct {
	Dl string `json:"download"`
}

func init() {
	region = os.Getenv("REGION")
	if len(region) == 0 {
		log.Fatal("region is empty")
	}
	downloadBucket = os.Getenv("DOWNLOAD_BUCKET")
	if len(downloadBucket) == 0 {
		log.Fatal("DOWNLOAD_BUCKET is empty")
	}
	uploadBucket = os.Getenv("UPLOAD_BUCKET")
	if len(uploadBucket) == 0 {
		log.Fatal("UPLOAD_BUCKET is empty")
	}
}

func HandleRequest(ctx context.Context, event LambdaEvent) (result string, err error) {
	if len(event.Dl) == 0 {
		err = fmt.Errorf("download is empty")
		return
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))

	now := time.Now().String()
	hash := md5.Sum([]byte(now))

	tmpPath := fmt.Sprintf("/tmp/%x", hash)
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return
	}
	defer tmp.Close()

	downloader := s3manager.NewDownloader(sess)
	size, err := downloader.Download(tmp, &s3.GetObjectInput{
		Bucket: aws.String(downloadBucket),
		Key:    aws.String(event.Dl),
	})
	if err != nil {
		return
	}
	log.Printf("downloaded size: %d", size)

	uploader := s3manager.NewUploader(sess)
	out, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(uploadBucket),
		Key:    aws.String(event.Dl),
		Body:   tmp,
	})
	if err != nil {
		return
	}
	log.Printf("Uploaded out: %+v", out)

	result = "Succeeded"
	return
}

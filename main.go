package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	log.Print("start")
	lambda.Start(HandleRequest)
}

type LambdaEvent struct {
	Key1 string `json:"key1"`
	Key2 string `json:"key2"`
	Key3 string `json:"key3"`
}

func HandleRequest(ctx context.Context, event LambdaEvent) (string, error) {
	log.Print("HandleRequest start")
	log.Print("keyi:", event.Key1, "key2:", event.Key2, "key3:", event.Key3)
	return "ok", nil
}

IMAGE=hapoon/lambda-go-template

.PHONY: docker-build	
docker-build:
	docker build -t ${IMAGE} .

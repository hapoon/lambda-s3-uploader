FROM golang:1.16-alpine3.13 as builder
WORKDIR /go/src
COPY . .
RUN go mod download

ARG CGO_ENABLED=0
ARG GOOS=linux
ARG GOARCH=amd64
RUN go build -o /go/bin/demo -ldflags '-s -w'

FROM amazon/aws-lambda-go as runner
COPY --from=builder /go/bin/demo /app/demo
ENTRYPOINT [ "/app/demo" ]
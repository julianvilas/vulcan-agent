FROM golang:1.13-alpine3.10 as builder

WORKDIR /app

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

# RUN go build -o vulcan-agent-docker -a -tags netgo -ldflags '-w' ./cmd/vulcan-agent-docker/main.go 
RUN go build -o vulcan-agent-kubernetes -a -tags netgo -ldflags '-w' ./cmd/vulcan-agent-kubernetes/main.go 

### App
FROM alpine:3.10

RUN apk add --no-cache --update gettext ca-certificates

WORKDIR /app

COPY --from=builder /app/vulcan-agent-kubernetes .
COPY config.toml .
COPY run.sh .

CMD ["/app/run.sh"]

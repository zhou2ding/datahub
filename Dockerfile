FROM golang:1.24.1-alpine AS builder

RUN apk add --no-cache make

COPY . /src

WORKDIR /src/apps/datahub

RUN GOPROXY=https://goproxy.cn make build

FROM alpine:latest

COPY --from=builder /src/apps/datahub/bin /app

WORKDIR /app

EXPOSE 10115

VOLUME ["/data/conf", "/data/logs"]

CMD ["./datahub", "-c", "/data/conf"]

FROM golang:1.25-alpine AS build

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=1 go build -buildvcs=false -o /bin/harness   ./cmd/harness
RUN CGO_ENABLED=1 go build -buildvcs=false -o /bin/harnessctl ./cmd/harnessctl
RUN CGO_ENABLED=1 go build -buildvcs=false -o /bin/mockmcp    ./cmd/mockmcp

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /bin/harness /bin/harnessctl /bin/mockmcp /usr/local/bin/
VOLUME /data
EXPOSE 9090
ENTRYPOINT ["harness"]
CMD ["--port", "9090", "--db", "/data/harness.db", "--mcp-cmd", "mockmcp"]

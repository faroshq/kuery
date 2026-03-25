FROM golang:1.24 AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o kuery ./cmd/kuery

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=builder /workspace/kuery /usr/local/bin/kuery

ENTRYPOINT ["kuery"]

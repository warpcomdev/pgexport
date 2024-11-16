FROM golang:1.23 AS builder

WORKDIR /app
COPY . .

ENV CGO_ENABLED=0
RUN go build

FROM scratch

COPY --from=builder /app/ /app/
ENTRYPOINT ["/app/pgexport"]
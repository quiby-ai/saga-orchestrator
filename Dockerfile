FROM golang:1.24-alpine AS build

RUN go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY sqlc.yaml ./
COPY internal/db/migrations/ ./internal/db/migrations/
COPY internal/storage/sqlc/queries/ ./internal/storage/sqlc/queries/

RUN sqlc generate

COPY . .

RUN CGO_ENABLED=0 go build -o /bin/app ./cmd/main.go

FROM gcr.io/distroless/static:nonroot
COPY --from=build /bin/app /app
USER nonroot

EXPOSE 8082
ENTRYPOINT ["/app"]

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ledger-service .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ledger-service /ledger-service
USER nonroot:nonroot
ENTRYPOINT ["/ledger-service"]

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=0 go build -o /out/payment-service .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/payment-service /payment-service
USER nonroot:nonroot
ENTRYPOINT ["/payment-service"]

# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/... ./examples/...

FROM alpine:3.24
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/ /usr/local/bin/
USER app
EXPOSE 8500 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8500/healthz || exit 1
ENTRYPOINT ["orchestrator"]

FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod .
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker .

FROM alpine:3.21
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/worker /usr/local/bin/worker
USER app
EXPOSE 8080
ENTRYPOINT ["worker"]


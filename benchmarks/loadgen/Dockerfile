FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge-loadgen .

FROM alpine:3.21
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/taskforge-loadgen /usr/local/bin/taskforge-loadgen
USER app
ENTRYPOINT ["taskforge-loadgen"]

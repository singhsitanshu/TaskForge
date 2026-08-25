FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/scheduler .

FROM alpine:3.21
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/scheduler /usr/local/bin/scheduler
USER app
EXPOSE 8080
ENTRYPOINT ["scheduler"]

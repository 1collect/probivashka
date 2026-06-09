FROM golang:1.26.2-alpine AS build

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /probivashka ./...

FROM alpine:3.22

WORKDIR /app

RUN apk add --no-cache ca-certificates \
	&& update-ca-certificates

COPY --from=build /probivashka /usr/local/bin/probivashka
COPY --from=build /app/index.html /app/index.html

ENV PORT=8888

EXPOSE 8888

CMD ["probivashka"]

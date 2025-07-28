FROM alpine:latest

WORKDIR /app

RUN apk update
RUN apk add cargo

COPY . /app


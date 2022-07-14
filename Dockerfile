FROM golang:1.18

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y poppler-utils wv unrtf tidy

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin/app ./...

CMD ["app"]

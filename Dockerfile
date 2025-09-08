FROM golang:1.23 as build
WORKDIR /go/src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/mongobetween .


FROM gcr.io/distroless/static-debian11
COPY --from=build /go/src/bin/mongobetween /mongobetween
ENTRYPOINT ["/mongobetween"]

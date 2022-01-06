FROM golang:1.17 as build
WORKDIR /go/src
COPY . .
RUN CGO_ENABLED=0 make


FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt \
     /etc/ssl/certs/ca-certificates.crt
COPY --from=build /go/src/bin/mongobetween /mongobetween
ENTRYPOINT ["/mongobetween"]

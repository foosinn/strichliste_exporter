FROM golang AS builder
WORKDIR /go/strichliste_exporter
ADD . .
ENV CGO_ENABLED=0
ENV GOPROXY=https://proxy.golang.org
RUN go build .

# ---

FROM busybox
COPY --from=builder /go/strichliste_exporter/strichliste_exporter /usr/local/bin/strichliste_exporter
ADD config.yml /etc/strichliste_exporter/config.yml
CMD strichliste_exporter

FROM golang AS builder

COPY . /opt/app/go/src/github.com/mixer/redplex/
ENV GOPATH=/opt/app/go/
WORKDIR /opt/app/go/src/github.com/mixer/redplex/
RUN go get github.com/kardianos/govendor && \
    ${GOPATH}/bin/govendor sync && \
    make redplex

FROM node:8-buster

COPY --from=builder /opt/app/go/src/github.com/mixer/redplex/redplex /usr/local/bin/redplex

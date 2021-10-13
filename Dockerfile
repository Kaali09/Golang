
FROM golang:1.13.10 AS builder
WORKDIR /work
COPY webhook.go /work
RUN go get github.com/google/go-github/github
RUN CGO_ENABLED=0 go build -o github-webhook-listener webhook.go

FROM scratch
COPY --from=builder /work/github-webhook-listener /
EXPOSE 8080
CMD ["/github-webhook-listener"]

GO_SRC = $(wildcard *.go)

redplex: $(GO_SRC)
	@printf " → Compiling %s \n" $@
	@go build ./cmd/$@
	@printf " ✔ Compiled %s \n" $@

lint: $(GO_SRC)
	@go vet ./ && printf " ✔ Vet passed \n"
	@golint ./ && printf " ✔ Lint passed \n"

check: $(GO_SRC) lint
	@go test -v -race ./
	@printf " ✔ Tests passed \n"

.PHONY: lint check

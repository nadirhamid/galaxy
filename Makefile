.SILENT :
.PHONY : commander galaxy clean fmt test upload-release

TAG:=`git describe --abbrev=0 --tags`
LDFLAGS:=-X main.buildVersion=`git describe --long --tags`

all: commander galaxy

deps:
	glock sync github.com/litl/galaxy

commander:
	echo "Building commander"
	go install -ldflags "$(LDFLAGS)" github.com/nadirhamid/galaxy/cmd/commander

commander-api:
	echo "Building commander API"
	go install -ldflags "$(LDFLAGS)" github.com/nadirhamid/galaxy/api/commander

galaxy:
	echo "Building galaxy"
	go install -ldflags "$(LDFLAGS)" github.com/nadirhamid/galaxy

clean: dist-clean
	rm -f $(GOPATH)/bin/commander
	rm -f $(GOPATH)/bin/galaxy

fmt:
	go fmt github.com/nadirhamid/galaxy/...

test:
	go test -v github.com/nadirhamid/galaxy/...

dist-clean:
	rm -rf dist
	rm -f galaxy-*.tar.gz

dist-init:
	mkdir -p dist/$$GOOS/$$GOARCH

dist-build: dist-init
	echo "Compiling $$GOOS/$$GOARCH"
	go build -ldflags "$(LDFLAGS)" -o dist/$$GOOS/$$GOARCH/galaxy github.com/nadirhamid/galaxy
	go build -ldflags "$(LDFLAGS)" -o dist/$$GOOS/$$GOARCH/commander github.com/nadirhamid/galaxy/cmd/commander

dist-linux-amd64:
	export GOOS="linux"; \
	export GOARCH="amd64"; \
	$(MAKE) dist-build

dist-linux-386:
	export GOOS="linux"; \
	export GOARCH="386"; \
	$(MAKE) dist-build

dist-darwin-amd64:
	export GOOS="darwin"; \
	export GOARCH="amd64"; \
	$(MAKE) dist-build

dist: dist-clean dist-init dist-linux-amd64 dist-linux-386 dist-darwin-amd64

release-tarball:
	echo "Building $$GOOS-$$GOARCH-$(TAG).tar.gz"
	GZIP=-9 tar -cvzf galaxy-$$GOOS-$$GOARCH-$(TAG).tar.gz -C dist/$$GOOS/$$GOARCH galaxy commander >/dev/null 2>&1

release-linux-amd64:
	export GOOS="linux"; \
	export GOARCH="amd64"; \
	$(MAKE) release-tarball

release-linux-386:
	export GOOS="linux"; \
	export GOARCH="386"; \
	$(MAKE) release-tarball

release-darwin-amd64:
	export GOOS="darwin"; \
	export GOARCH="amd64"; \
	$(MAKE) release-tarball

release: deps dist release-linux-amd64 release-linux-386 release-darwin-amd64

upload-release:
	aws s3 cp galaxy-darwin-amd64-$(TAG).tar.gz s3://litl-package-repo/galaxy/galaxy-darwin-amd64-$(TAG).tar.gz --acl public-read
	aws s3 cp galaxy-linux-amd64-$(TAG).tar.gz s3://litl-package-repo/galaxy/galaxy-linux-amd64-$(TAG).tar.gz --acl public-read
	aws s3 cp galaxy-linux-386-$(TAG).tar.gz s3://litl-package-repo/galaxy/galaxy-linux-386-$(TAG).tar.gz --acl public-read
	echo https://s3.amazonaws.com/litl-package-repo/galaxy/galaxy-darwin-amd64-$(TAG).tar.gz
	echo https://s3.amazonaws.com/litl-package-repo/galaxy/galaxy-linux-amd64-$(TAG).tar.gz
	echo https://s3.amazonaws.com/litl-package-repo/galaxy/galaxy-linux-386-$(TAG).tar.gz


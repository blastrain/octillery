.PHONY: install_command

install_command:
	go install go.knocknote.io/octillery/cmd/octillery

.PHONY: test
test:
	go test -v -cover ./...

.PHONY: update-deps
update-deps: download update_vendor transpose

update_vendor:
	glide up

.PHONY: deps
deps: install_vendor transpose

install_vendor: download
	glide install

download:
	which -s glide || (curl https://glide.sh/get | sh)

transpose: uninstall_plugin transpose_vendor install_plugin

transpose_vendor:
	go run cmd/octillery/main.go transpose vendor

install_plugin:
	go run cmd/octillery/main.go install --sqlite

uninstall_plugin:
	rm -f plugin/{mysql,sqlite3}.go

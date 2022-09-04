SOURCES=$(shell find src -type f)

build: $(shell find src -type f)
	nimble build

docs: $(shell find src -type f)
	nim doc --project --index:on --git.url:https://github.com/jdbernard/fiber-orm --outdir:docs src/fiber_orm
	nim rst2html --outdir:docs README.rst
	cp docs/fiber_orm.html docs/index.html
.PHONY: docs

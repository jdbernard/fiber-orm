SOURCES=$(shell find src -type f)

doc: $(shell find src -type f)
	nim doc --project --index:on --git.url:https://github.com/jdbernard/fiber-orm --outdir:htmdocs src/fiber_orm
	nim rst2html --outdir:htmdocs README.rst

.PHONY: doc

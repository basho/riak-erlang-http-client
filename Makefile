.PHONY: all lint clean compile deps distclean release docs

PROJDIR := $(realpath $(CURDIR))
REBAR := $(PROJDIR)/rebar

all: deps compile

lint: xref dialyzer

compile: deps
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

distclean: clean
	$(REBAR) delete-deps

release: compile
ifeq ($(VERSION),)
	$(error VERSION must be set to build a release and deploy this package)
endif
ifeq ($(RELEASE_GPG_KEYNAME),)
	$(error RELEASE_GPG_KEYNAME must be set to build a release and deploy this package)
endif
	@echo "==> Tagging version $(VERSION)"
	@./tools/build/publish $(VERSION) master validate
	@git tag --sign -a "$(VERSION)" -m "riak-erlang-http-client $(VERSION)" --local-user "$(RELEASE_GPG_KEYNAME)"
	@git push --tags
	@./tools/build/publish $(VERSION) master 'Riak Erlang HTTP Client' 'riak-erlang-http-client'

DIALYZER_APPS = kernel stdlib crypto ibrowse

include tools.mk

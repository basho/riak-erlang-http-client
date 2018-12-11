.PHONY: rel deps doc

all: deps
	@./rebar compile

deps:
	@./rebar get-deps

clean:
	@./rebar clean

test: all
		@./rebar skip_deps=true eunit

distclean: clean
	@./rebar delete-deps

doc:
	@./rebar doc skip_deps=true

compile: deps
	@./rebar compile

# Erlang-specific build steps
DIALYZER_APPS = kernel stdlib erts crypto compiler hipe syntax_tools
include tools.mk

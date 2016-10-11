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

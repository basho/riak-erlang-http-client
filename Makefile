.PHONY: rel deps doc

all: deps
	@./rebar compile

deps:
	@./rebar get-deps

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps

doc:
	erl -noshell -run edoc_run application riakhttpc '"."' '[]'

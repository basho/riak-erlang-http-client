.PHONY: rel deps doc

PROJDIR := $(realpath $(CURDIR))
REBAR := $(PROJDIR)/rebar3

all: deps compile

lint: xref dialyzer

compile: deps
	$(REBAR) compile

deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean

test: all
	@$(REBAR) eunit

distclean: clean
	@$(REBAR) clean --all

doc:
	@$(REBAR) edoc

dialyzer:
	@$(REBAR) dialyzer

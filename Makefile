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
	@./rebar doc skip_deps=true

COMBO_PLT = $(HOME)/.rhc_dialyzer_plt
APPS = kernel stdlib sasl erts eunit
INCLUDES = -I include -I deps
check_plt: all
	dialyzer --check_plt --plt $(COMBO_PLT) --apps $(APPS) ./deps/*/ebin

build_plt: all
	dialyzer --build_plt --output_plt $(COMBO_PLT) --apps $(APPS) ./deps/*/ebin

dialyzer: all
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	@sleep 1
	dialyzer --verbose -Wno_return --plt $(COMBO_PLT) $(INCLUDES) ebin

typer: $(DEPSOLVER_PLT)
	typer --plt $(COMBO_PLT) -r ./src

plt_info:
	dialyzer --plt $(COMBO_PLT) --plt_info

cleanplt:
	@echo
	@echo "Are you sure?  It takes time to re-build."
	@echo Deleting $(COMBO_PLT) in 5 seconds.
	@echo
	@sleep 5
	rm $(COMBO_PLT)
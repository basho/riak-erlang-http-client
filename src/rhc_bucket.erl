%% @doc This module contains utilities that the rhc module uses to
%%      encode and decode bucket-property requests.
-module(rhc_bucket).

-export([erlify_props/1,
         httpify_props/1]).

-include("raw_http.hrl").

erlify_props(Props) ->
    lists:flatten([ erlify_prop(K, V) || {K, V} <- Props ]).
erlify_prop(?JSON_N_VAL, N) -> {n_val, N};
erlify_prop(?JSON_ALLOW_MULT, AM) -> {allow_mult, AM};
erlify_prop(_Ignore, _) -> [].

httpify_props(Props) ->
    lists:flatten([ httpify_prop(K, V) || {K, V} <- Props ]).
httpify_prop(n_val, N) -> {?JSON_N_VAL, N};
httpify_prop(allow_mult, AM) -> {?JSON_ALLOW_MULT, AM};
httpify_prop(_Ignore, _) -> [].

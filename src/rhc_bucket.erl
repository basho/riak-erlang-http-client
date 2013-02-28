%% -------------------------------------------------------------------
%%
%% riakhttpc: Riak HTTP Client
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc This module contains utilities that the rhc module uses to
%%      encode and decode bucket-property requests.
-module(rhc_bucket).

-export([erlify_props/1,
         httpify_props/1]).

-include("raw_http.hrl").

erlify_props(Props) ->
    lists:flatten([ erlify_prop(K, V) || {K, V} <- Props ]).
erlify_prop(?JSON_ALLOW_MULT, AM) -> {allow_mult, AM};
erlify_prop(?JSON_BACKEND, B) -> {backend, B};
erlify_prop(?JSON_BASIC_Q, B) -> {basic_quorum, B};
erlify_prop(?JSON_BIG_VC, I) -> {big_vclock, I};
erlify_prop(?JSON_CHASH, CH) -> {chash_keyfun, erlify_chash(CH)};
erlify_prop(?JSON_DW, DW) -> {dw, erlify_quorum(DW)};
erlify_prop(?JSON_LINKFUN, LF) -> {linkfun, erlify_linkfun(LF)};
erlify_prop(?JSON_LWW, LWW) -> {last_write_wins, LWW};
erlify_prop(?JSON_NF_OK, NF) -> {notfound_ok, NF};
erlify_prop(?JSON_N_VAL, N) -> {n_val, N};
erlify_prop(?JSON_OLD_VC, I) -> {old_vclock, I};
erlify_prop(?JSON_POSTCOMMIT, P) -> {postcommit, P};
erlify_prop(?JSON_PR, PR) -> {dw, erlify_quorum(PR)};
erlify_prop(?JSON_PRECOMMIT, P) -> {precommit, P};
erlify_prop(?JSON_PW, PW) -> {pw, erlify_quorum(PW)};
erlify_prop(?JSON_R, R) -> {r, erlify_quorum(R)};
erlify_prop(?JSON_REPL, R) -> {repl, erlify_repl(R)};
erlify_prop(?JSON_RW, RW) -> {rw, erlify_quorum(RW)};
erlify_prop(?JSON_SEARCH, B) -> {search, B};
erlify_prop(?JSON_SMALL_VC, I) -> {small_vclock, I};
erlify_prop(?JSON_W, W) -> {w, erlify_quorum(W)};
erlify_prop(?JSON_YOUNG_VC, I) -> {young_vclock, I};
erlify_prop(_Ignore, _) -> [].

erlify_quorum(?JSON_ALL) -> all;
erlify_quorum(?JSON_QUORUM) -> quorum;
erlify_quorum(?JSON_ONE) -> one;
erlify_quorum(I) when is_integer(I) -> I;
erlify_quorum(_) -> undefined.

erlify_repl(?JSON_REALTIME) -> realtime;
erlify_repl(?JSON_FULLSYNC) -> fullsync;
erlify_repl(?JSON_BOTH) -> true; %% both is equivalent to true, but only works in 1.2+
erlify_repl(true) -> true;
erlify_repl(false) -> false;
erlify_repl(_) -> undefined.

erlify_chash({struct, [{?JSON_MOD, Mod}, {?JSON_FUN, Fun}]}=Struct) ->
    try
        {binary_to_existing_atom(Mod, utf8), binary_to_existing_atom(Fun, utf8)}
    catch
        error:badarg ->
            error_logger:warning_msg("Creating modfun atoms from JSON bucket property! ~p", [Struct]),
            {binary_to_atom(Mod, utf8), binary_to_atom(Fun, utf8)}
    end.

erlify_linkfun(Struct) ->
    {Mod, Fun} = erlify_chash(Struct),
    {modfun, Mod, Fun}.

httpify_props(Props) ->
    lists:flatten([ httpify_prop(K, V) || {K, V} <- Props ]).
httpify_prop(allow_mult, AM) -> {?JSON_ALLOW_MULT, AM};
httpify_prop(backend, B) -> {?JSON_BACKEND, B};
httpify_prop(basic_quorum, B) -> {?JSON_BASIC_Q, B};
httpify_prop(big_vclock, VC) -> {?JSON_BIG_VC, VC};
httpify_prop(chash_keyfun, MF) -> {?JSON_CHASH, httpify_modfun(MF)};
httpify_prop(dw, Q) -> {?JSON_DW, Q};
httpify_prop(last_write_wins, LWW) -> {?JSON_LWW, LWW};
httpify_prop(linkfun, LF) -> {?JSON_LINKFUN, httpify_modfun(LF)};
httpify_prop(n_val, N) -> {?JSON_N_VAL, N};
httpify_prop(notfound_ok, NF) -> {?JSON_NF_OK, NF};
httpify_prop(old_vclock, VC) -> {?JSON_OLD_VC, VC};
httpify_prop(postcommit, P) -> {?JSON_POSTCOMMIT, P};
httpify_prop(pr, Q) -> {?JSON_PR, Q};
httpify_prop(precommit, P) -> {?JSON_PRECOMMIT, P};
httpify_prop(pw, Q) -> {?JSON_PW, Q};
httpify_prop(r, Q) -> {?JSON_R, Q};
httpify_prop(repl, R) -> {?JSON_REPL, R};
httpify_prop(rw, Q) -> {?JSON_RW, Q};
httpify_prop(search, B) -> {?JSON_SEARCH, B};
httpify_prop(small_vclock, VC) -> {?JSON_SMALL_VC, VC};
httpify_prop(w, Q) -> {?JSON_W, Q};
httpify_prop(young_vclock, VC) -> {?JSON_YOUNG_VC, VC};
httpify_prop(_Ignore, _) -> [].

httpify_modfun({modfun, M, F}) ->
    httpify_modfun({M, F});
httpify_modfun({M, F}) ->
    {struct, [{?JSON_MOD, atom_to_binary(M, utf8)},
              {?JSON_FUN, atom_to_binary(F, utf8)}]}.

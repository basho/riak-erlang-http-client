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
erlify_prop(?JSON_N_VAL, N) -> {n_val, N};
erlify_prop(?JSON_ALLOW_MULT, AM) -> {allow_mult, AM};
erlify_prop(?JSON_PRECOMMIT, P) -> {precommit, P};
erlify_prop(?JSON_POSTCOMMIT, P) -> {postcommit, P};
erlify_prop(_Ignore, _) -> [].

httpify_props(Props) ->
    lists:flatten([ httpify_prop(K, V) || {K, V} <- Props ]).
httpify_prop(n_val, N) -> {?JSON_N_VAL, N};
httpify_prop(allow_mult, AM) -> {?JSON_ALLOW_MULT, AM};
httpify_prop(precommit, P) -> {?JSON_PRECOMMIT, P};
httpify_prop(postcommit, P) -> {?JSON_POSTCOMMIT, P};
httpify_prop(_Ignore, _) -> [].

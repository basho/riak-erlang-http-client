%% -------------------------------------------------------------------
%%
%% riakhttpc: Riak HTTP Client
%%
%% Copyright (c) 2013 Basho Technologies, Inc. All Rights Reserved.
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

%% @doc Utility functions for datatypes.
-module(rhc_dt).

-export([
         datatype_from_json/1,
         encode_update_request/3,
         decode_error/2
        ]).

-define(FIELD_PATTERN, "^(.*)_(counter|set|register|flag|map)$").

datatype_from_json({struct, Props}) ->
    Value = proplists:get_value(<<"value">>, Props),
    Type = binary_to_existing_atom(proplists:get_value(<<"type">>, Props), utf8),
    Context = proplists:get_value(<<"context">>, Props, undefined),
    Mod = riakc_datatype:module(Type),
    Mod:new(decode_value(Type, Value), Context).

decode_value(counter, Value) -> Value;
decode_value(set, Value) -> Value;
decode_value(flag, Value) -> Value;
decode_value(register, Value) -> Value;
decode_value(map, {struct, Fields}) ->
    [ begin
          {Name, Type} = field_from_json(Field),
          {{Name,Type}, decode_value(Type, Value)}
      end || {Field, Value} <- Fields ].

field_from_json(Bin) when is_binary(Bin) ->
    {match, [Name, BinType]} = re:run(Bin, ?FIELD_PATTERN, [anchored, {capture, all_but_first, binary}]),
    {Name, binary_to_existing_atom(BinType, utf8)}.

field_to_json({Name, Type}) when is_binary(Name), is_atom(Type) ->
    BinType = atom_to_binary(Type, utf8),
    <<Name/bytes, $_, BinType/bytes>>.

decode_error(fetch, {ok, "404", Headers, Body}) ->
    case proplists:get_value("Content-Type", Headers) of
        "application/json" ->
            %% We need to extract the type when not found
            {struct, Props} = mochijson2:decode(Body),
            Type = binary_to_existing_atom(proplists:get_value(<<"type">>, Props), utf8),
            {notfound, Type};
         "text/" ++ _ ->
            Body
    end;
decode_error(_, {ok, "400", _, Body}) ->
    {bad_request, Body};
decode_error(_, {ok, "301", _, Body}) ->
    {legacy_counter, Body};
decode_error(_, {ok, "403", _, Body}) ->
    {forbidden, Body};
decode_error(_, {ok, _, _, Body}) ->
    Body.

encode_update_request(register, {assign, Bin}, _Context) ->
    {struct, [{<<"assign">>, Bin}]};
encode_update_request(flag, Atom, _Context) ->
    atom_to_binary(Atom, utf8);
encode_update_request(counter, Op, _Context) ->
    {struct, [Op]};
encode_update_request(set, {update, Ops}, Context) ->
    {struct, Ops ++ include_context(Context)};
encode_update_request(set, Op, Context) ->
    {struct, [Op|include_context(Context)]};
encode_update_request(map, {update, Ops}, Context) ->
    {struct, orddict:to_list(lists:foldl(fun encode_map_op/2, orddict:new(), Ops)) ++ 
             include_context(Context)}.

encode_map_op({add, Entry}, Ops) ->
    orddict:append(add, field_to_json(Entry), Ops);
encode_map_op({remove, Entry}, Ops) ->
    orddict:append(remove, field_to_json(Entry), Ops);
encode_map_op({update, {_Key,Type}=Field, Op}, Ops) ->
    EncOp = encode_update_request(Type, Op, undefined),
    Update = {field_to_json(Field), EncOp},
    case orddict:find(update, Ops) of
        {ok, {struct, Updates}} ->
            orddict:store(update, {struct, [Update|Updates]}, Ops);
        error ->
            orddict:store(update, {struct, [Update]}, Ops)
    end.

include_context(undefined) -> [];
include_context(<<>>) -> [];
include_context(Bin) -> [{<<"context">>, Bin}].


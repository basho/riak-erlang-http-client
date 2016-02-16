%% -------------------------------------------------------------------
%%
%% riakhttpc: Riak HTTP Client
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Riak TS Erlang HTTP Client.  This module provides access to Riak's
%%      HTTP interface.  For basic usage, please read
%%      <a href="overview.html">the riakhttpc application overview</a>.

-module(rhc_ts).

-export([create/0, create/3,
         ip/1,
         port/1,
         options/1,
         get_client_id/1,
         get/3, get/4,
         put/3, put/4,
         delete/3, delete/4,
         list_keys/2,
         stream_list_keys/2,
         query/2
         ]).

-include("raw_http.hrl").
-include("rhc.hrl").
-include_lib("riakc/include/riakc.hrl").

-export_type([rhc/0]).
-opaque rhc() :: #rhc{}.

-type ts_table()  :: binary().
-type ts_key()    :: [integer() | float() | binary()].
-type ts_record() :: [integer() | float() | binary()].
-type ts_selection() :: {Columns::[binary()], Rows::[ts_record()]}.

-define(API_VERSION, "v1").


%% @doc Create a client for connecting to the default port on localhost.
%% @equiv create("127.0.0.1", 8098, []).
create() ->
    create("127.0.0.1", 8098, []).

%% @doc Create a client for connecting to a Riak node.
-spec create(string(), integer(), Options::list()) -> rhc().
create(IP, Port, Opts0) when is_list(IP), is_integer(Port),
                             is_list(Opts0) ->
    Opts = case proplists:lookup(client_id, Opts0) of
               none -> [{client_id, random_client_id()}|Opts0];
               Bin when is_binary(Bin) ->
                   [{client_id, binary_to_list(Bin)}
                    | [ O || O={K,_} <- Opts0, K =/= client_id ]];
               _ ->
                   Opts0
           end,
    ApiVersion = proplists:get_value(api_version, Opts, ?API_VERSION),
    Opts1 = lists:keystore(api_version, 1, Opts, {api_version, ApiVersion}),
    #rhc{ip = IP, port = Port, prefix = "/ts/"++ApiVersion, options = Opts1}.

%% @doc Get the IP this client will connect to.
%% @spec ip(rhc()) -> string()
ip(#rhc{ip=IP}) -> IP.

%% @doc Get the Port this client will connect to.
%% @spec port(rhc()) -> integer()
port(#rhc{port=Port}) -> Port.

%% @doc Get the client ID that this client will use when storing objects.
%% @spec get_client_id(rhc()) -> {ok, string()}
get_client_id(Rhc) ->
    {ok, client_id(Rhc, [])}.


-spec get(rhc(), ts_table(), ts_key()) ->
                 {ok, ts_selection()} | {error, term()}.
%% @equiv get(Rhc, Table, Key, [])
get(Rhc, Table, Key) ->
    get(Rhc, Table, Key, []).

-spec get(rhc(), Table::ts_table(), Key::ts_key(), Options::proplists:proplist()) ->
                 {ok, ts_selection()} | {error, term()}.
%% @doc Get the TS record stored in Table at Key.
%%      Takes a value for timeout in Options.
get(Rhc, Table, Key, Options) ->
    Encoded = mochijson2:encode({struct, [{key, Key}]}),
    Qs = [{json, Encoded} | get_q_params(Rhc, Options)],
    Url = make_get_url(Rhc, Table, Qs),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
    case request(get, Url, ["200"], Headers, [], Rhc) of
        {ok, _Status, _Headers, Body} ->
            case catch mochijson2:decode(Body) of
                {struct, [{<<"columns">>, Columns}, {<<"rows">>, Rows}]} ->
                    {ok, {Columns, Rows}};
                _ ->
                    {error, bad_body}
            end;
        {error, {ok, "404", _, _}} ->
            {error, notfound};
        {error, Error} ->
            {error, Error}
    end.

-spec put(rhc(), ts_table(), [ts_record()]) -> ok | {error, term()}.
put(Rhc, Table, Records) ->
    put(Rhc, Table, Records, []).

-spec put(rhc(), ts_table(), Batch::[ts_record()], proplists:proplist()) ->
                 ok | {error, term()}.
%% @doc Batch put of TS records.
put(Rhc, Table, Batch, Options) ->
    Encoded = mochijson2:encode({struct, [{data, Batch}]}),
    Qs = put_q_params(Rhc, Options),
    Url = make_put_url(Rhc, Table, Qs),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
    case request(put, Url, ["415"], Headers, Encoded, Rhc) of
        {ok, _Status, _, <<"ok">>} ->
            ok;
        {error, {ok, "404", _, _}} ->
            {error, notfound};
        {error, Error} ->
            {error, Error}
    end.


-spec delete(rhc(), ts_table(), ts_key()) -> ok | {error, term()}.
%% @equiv delete(Rhc, Table, Key, [])
delete(Rhc, Table, Key) ->
    delete(Rhc, Table, Key, []).

-spec delete(rhc(), ts_table(), ts_key(), proplists:proplist()) ->
                    ok | {error, term()}.
%% @doc Delete the given key from the given bucket.
delete(Rhc, Table, Key, Options) ->
    Encoded = mochijson2:encode({struct, [{key, Key}]}),
    Qs = delete_q_params(Rhc, Options),
    Url = make_delete_url(Rhc, Table, Qs),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
    case request(delete, Url, ["200"], Headers, Encoded, Rhc) of
        {ok, "200", _Headers, <<"ok">>} ->
            ok;
        {error, {ok, "404", _, _}} ->
            {error, notfound};
        {error, {ok, "400", _, _}} ->
            {error, bad_key};
        {error, Error} ->
            {error, Error}
    end.


list_keys(Rhc, Table) ->
    {ok, ReqId} = stream_list_keys(Rhc, Table),
    case rhc_listkeys:wait_for_list(ReqId, ?DEFAULT_TIMEOUT) of
        {error, {"404", _Headers}} ->
            {error, notfound};
        Result ->
            Result
    end.

-spec stream_list_keys(rhc(), ts_table()) ->
          {ok, reference()} | {error, term()}.
%% @doc Stream key lists to a Pid.
stream_list_keys(Rhc, Table) ->
    Url = lists:flatten([root_url(Rhc),"/tables/",binary_to_list(Table),"/keys"]),
    StartRef = make_ref(),
    Pid = spawn(rhc_listkeys, list_acceptor, [self(), StartRef, ts_keys]),
    case request_stream(Pid, get, Url, [], [], Rhc) of
        {ok, ReqId} ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} ->
            {error, Error}
    end.


-spec query(rhc(), string()) -> {ok, ts_selection()} | {error, term()}.
query(Rhc, Query) ->
    query(Rhc, Query, []).

-spec query(rhc(), string(), proplists:proplist()) ->
                   ts_selection() | {error, term()}.
query(Rhc, Query, Options) ->
    %% queries should be dispatched with different methods depending
    %% on query species
    {match, [FirstWord]} =
        re:run(Query, "([[:alpha:]]+)",
               [{capture, all_but_first, list}]),
    Encoded = mochijson2:encode({struct, [{query, list_to_binary(Query)}]}),
    {Method, Qs, Body} =
        case string:to_lower(FirstWord) of
            "select" ->
                {get,  [{json, iolist_to_binary(Encoded)}], []};
            "create" ->
                {post, [], Encoded};
            "describe" ->
                {get,  [{json, iolist_to_binary(Encoded)}], []}
        end,
    Url = lists:flatten([root_url(Rhc),"/query?",mochiweb_util:urlencode(Qs)]),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
    case request(Method, Url, ["200", "204"], Headers, Body, Rhc) of
        {ok, "200", _Headers, BodyJson} ->
            case catch mochijson2:decode(BodyJson) of
                {struct, [{<<"columns">>, Columns}, {<<"rows">>, Rows}]} ->
                    %% convert records (coming in as lists) to tuples
                    %% to conform to the representation used in riakc
                    {Columns, [list_to_tuple(R) || R <- Rows]};
                _ ->
                    {error, bad_body}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% ------------------------
%% supporting functions

%% @doc Get the client ID to use, given the passed options and client.
%%      Choose the client ID in Options before the one in the client.
%% @spec client_id(rhc(), proplist()) -> client_id()
client_id(#rhc{options=RhcOptions}, Options) ->
    case proplists:get_value(client_id, Options) of
        undefined ->
            proplists:get_value(client_id, RhcOptions);
        ClientId ->
            ClientId
    end.

%% @doc Generate a random client ID.
%% @spec random_client_id() -> client_id()
random_client_id() ->
    Id = crypto:rand_bytes(32),
    integer_to_list(erlang:phash2(Id)).

%% @doc Assemble the root URL for the given client
%% @spec root_url(rhc()) -> iolist()
root_url(#rhc{ip = Ip, port = Port,
              prefix = Prefix, options = Opts}) ->
    Proto = case proplists:get_value(is_ssl, Opts) of
        true ->
            "https";
        _ ->
            "http"
    end,
    [Proto, "://",Ip,$:,integer_to_list(Port),$/,Prefix].


%% @doc send an ibrowse request
request(Method, Url, Expect, Headers, Body, Rhc) ->
    AuthHeader = get_auth_header(Rhc#rhc.options),
    SSLOptions = get_ssl_options(Rhc#rhc.options),
    Accept = {"Accept", "multipart/mixed, */*;q=0.9"},
    case ibrowse:send_req(Url, [Accept|Headers] ++ AuthHeader, Method, Body,
                          [{response_format, binary}] ++ SSLOptions) of
        Resp = {ok, Status, _, _} ->
            case lists:member(Status, Expect) of
                true -> Resp;
                false -> {error, Resp}
            end;
        Error ->
            Error
    end.

%% @doc stream an ibrowse request
request_stream(Pid, Method, Url, Headers, Body, Rhc) ->
    AuthHeader = get_auth_header(Rhc#rhc.options),
    SSLOptions = get_ssl_options(Rhc#rhc.options),
    Accept = {"Accept", "multipart/mixed, */*;q=0.9"},
    case ibrowse:send_req(Url, [Accept | Headers] ++ AuthHeader, Method, Body,
                          [{stream_to, Pid},
                           {response_format, binary}] ++ SSLOptions) of
        {ibrowse_req_id, ReqId} ->
            {ok, ReqId};
        Error ->
            Error
    end.

%% @doc Get the default options for the given client
%% @spec options(rhc()) -> proplist()
options(#rhc{options=Options}) ->
    Options.

%% @doc Extract the list of query parameters to use for a GET
%% @spec get_q_params(rhc(), proplist()) -> proplist()
get_q_params(Rhc, Options) ->
    options_list([timeout], Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a PUT
%% @spec put_q_params(rhc(), proplist()) -> proplist()
put_q_params(Rhc, Options) ->
    options_list([],
                 Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a DELETE
%% @spec delete_q_params(rhc(), proplist()) -> proplist()
delete_q_params(Rhc, Options) ->
    options_list([timeout], Options ++ options(Rhc)).


%% @doc Extract the options for the given `Keys' from the possible
%%      list of `Options'.
%% @spec options_list([Key::atom()|{Key::atom(),Alias::string()}],
%%                    proplist()) -> proplist()
options_list(Keys, Options) ->
    options_list(Keys, Options, []).

options_list([K|Rest], Options, Acc) ->
    {Key,Alias} = case K of
                      {_, _} -> K;
                      _ -> {K, K}
                  end,
    NewAcc = case proplists:lookup(Key, Options) of
                 {Key,V} -> [{Alias,V}|Acc];
                 none  -> Acc
             end,
    options_list(Rest, Options, NewAcc);
options_list([], _, Acc) ->
    Acc.

get_auth_header(Options) ->
    case lists:keyfind(credentials, 1, Options) of
        {credentials, User, Password} ->
            [{"Authorization",
              "Basic " ++ base64:encode_to_string(User ++ ":" ++ Password)}];
        _ ->
            []
    end.

get_ssl_options(Options) ->
    case proplists:get_value(is_ssl, Options) of
        true ->
            [{is_ssl, true}] ++ case proplists:get_value(ssl_options, Options, []) of
                X when is_list(X) ->
                    [{ssl_options, X}];
                _ ->
                    [{ssl_options, []}]
            end;
        _ ->
            []
    end.

make_get_url(Rhc, Table, Qs) ->
    make_nonq_url(Rhc, Table, Qs).
make_delete_url(Rhc, Table, Qs) ->
    make_nonq_url(Rhc, Table, Qs).
make_put_url(Rhc, Table, Qs) ->
    make_nonq_url(Rhc, Table, Qs).
make_nonq_url(Rhc, Table, Qs_) ->
    Qs = [{K, iolist_to_binary(V)} || {K,V} <- Qs_],
    lists:flatten(
      [root_url(Rhc),
       "/tables/", binary_to_list(Table),
       [[$?, mochiweb_util:urlencode(Qs)] || Qs /= []]]).

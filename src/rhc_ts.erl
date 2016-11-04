%% -------------------------------------------------------------------
%%
%% rhc_ts: TS extensions for Riak HTTP Client
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

-type ts_table()  :: binary() | string().
-type ts_key()    :: [integer() | float() | binary()].
-type ts_record() :: [integer() | float() | binary()].
-type ts_selection() :: {Columns::[binary()], Rows::[ts_record()]}.

-define(API_VERSION, "v1").


%% @doc Create a client for connecting to the default port on localhost.
%% @equiv create("127.0.0.1", 8098, []).
create() ->
    create("127.0.0.1", 8098, []).

%% @doc Create a client for connecting to a Riak node.
-spec create(string(), inet:port_number(), Options::list()) ->
                    #rhc{}.
%% @doc Create a client for connecting to IP:Port, with predefined
%%      request parameters and client options in Options, specially including:
%%       api_version :: string(), to insert after /ts/;
%%       client_id :: string(), to use in request headers;
create(IP, Port, Options0)
  when is_list(IP), is_integer(Port), is_list(Options0) ->
    Options =
        lists:foldl(
          fun({Key, Default, ValidF}, AccOpts) ->
                  lists:keystore(
                    Key, 1, AccOpts,
                    {Key, ValidF(proplists:get_value(Key, AccOpts, Default))})
          end,
          Options0,
          [{api_version, ?API_VERSION,
            fun(X) when is_list(X) -> X end},
           {client_id, random_client_id(),
            fun(X) when is_list(X) -> X end}]),
    #rhc{ip = IP, port = Port,
         prefix = "/ts/"++proplists:get_value(api_version, Options),
         options = Options}.

%% @doc Get the IP this client will connect to.
%% @spec ip(#rhc{}) -> string()
ip(#rhc{ip=IP}) -> IP.

%% @doc Get the Port this client will connect to.
%% @spec port(#rhc{}) -> integer()
port(#rhc{port=Port}) -> Port.

%% @doc Get the client ID that this client will use when storing objects.
%% @spec get_client_id(#rhc{}) -> {ok, string()}
get_client_id(Rhc) ->
    {ok, client_id(Rhc, [])}.


-spec get(#rhc{}, ts_table(), ts_key()) ->
                 {ok, ts_selection()} | {error, term()}.
%% @equiv get(Rhc, Table, Key, [])
get(Rhc, Table, Key) ->
    get(Rhc, Table, Key, []).

-spec get(#rhc{}, ts_table(), ts_key(), Options::proplists:proplist()) ->
                 {ok, ts_selection()} | {error, notfound | {integer(), binary()} | term()}.
%% @doc Get the TS record stored in Table at Key.
%%      Takes a value for timeout in Options.
get(Rhc, Table, Key, Options) when is_binary(Table) ->
    get(Rhc, binary_to_list(Table), Key, Options);
get(Rhc, Table, Key, Options) ->
    Qs = get_q_params(Rhc, Options),
    case make_get_url(Rhc, Table, Key, Qs, Options) of
        {ok, Url} ->
            Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
            case request(get, Url, ["200"], Headers, [], Rhc) of
                {ok, _Status, _Headers, Body} ->
                    case catch mochijson2:decode(Body) of
                        {struct, [{<<"columns">>, Columns}, {<<"rows">>, Rows}]} ->
                            {ok, {Columns, Rows}};
                        _ ->
                            {error, bad_body}
                    end;
                {ok, "404", _, _} ->
                    {error, notfound};
                {_, Code, _, Body} when is_list(Code) ->
                    {error, {list_to_integer(Code), Body}};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec put(#rhc{}, ts_table(), [ts_record()]) -> ok | {error, {integer(), binary()} | term()}.
put(Rhc, Table, Records) ->
    put(Rhc, Table, Records, []).

-spec put(#rhc{}, ts_table(), Batch::[ts_record()], proplists:proplist()) ->
                 ok | {error, {integer(), binary()} | term()}.
%% @doc Batch put of TS records.
put(Rhc, Table, Batch, Options) when is_binary(Table) ->
    put(Rhc, binary_to_list(Table), Batch, Options);
put(Rhc, Table, Batch, Options) ->
    Encoded = mochijson2:encode(Batch),
    Qs = put_q_params(Rhc, Options),
    {ok, Url} = make_put_url(Rhc, Table, Qs),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
    case request(post, Url, ["200", "401", "400"], Headers, Encoded, Rhc) of
        {ok, "200", _, <<"ok">>} ->
            ok;
        {ok, "200", _, <<"{\"success\":true}">>} ->
            ok;
        {ok, "404", _, _} ->
            {error, notfound};
        {_, Code, _, Body} when is_list(Code) ->
            {error, {list_to_integer(Code), Body}};
        {error, Error} ->
            {error, Error}
    end.


-spec delete(#rhc{}, ts_table(), ts_key()) -> ok | {error, {integer(), binary()} | term()}.
%% @equiv delete(Rhc, Table, Key, [])
delete(Rhc, Table, Key) ->
    delete(Rhc, Table, Key, []).

-spec delete(#rhc{}, ts_table(), ts_key(), proplists:proplist()) ->
                    ok | {error, notfound | bad_key | {integer(), binary()} | term()}.
%% @doc Delete the given key from the given bucket.
delete(Rhc, Table, Key, Options) when is_binary(Table) ->
    delete(Rhc, binary_to_list(Table), Key, Options);
delete(Rhc, Table, Key, Options) ->
    Qs = delete_q_params(Rhc, Options),
    case make_delete_url(Rhc, Table, Key, Qs, Options) of
        {ok, Url} ->
            Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
            case request(delete, Url, ["200", "401", "400"], Headers, [], Rhc) of
                {ok, "200", _Headers, <<"ok">>} ->
                    ok;
                {ok, _Status, _, <<"{\"success\":true}">>} ->
                    ok;
                {ok, "404", _, _} ->
                    {error, notfound};
                {ok, "400", _, _} ->
                    {error, bad_key};
                {_, Code, _, Body} when is_list(Code) ->
                    {error, {list_to_integer(Code), Body}};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


-spec list_keys(#rhc{}, ts_table()) -> {ok, [ts_key()]} | {error, notfound}.
list_keys(Rhc, Table) ->
    {ok, ReqId} = stream_list_keys(Rhc, Table),
    case rhc_listkeys:wait_for_list(ReqId, ?DEFAULT_TIMEOUT) of
        {ok, {"404", _Headers}} ->
            {error, notfound};
        Result ->
            Result
    end.

-spec stream_list_keys(#rhc{}, ts_table()) ->
          {ok, reference()} | {error, term()}.
%% @doc Stream key lists to a Pid.
stream_list_keys(Rhc, Table) when is_binary(Table) ->
    stream_list_keys(Rhc, binary_to_list(Table));
stream_list_keys(Rhc, Table) ->
    Url = lists:flatten([root_url(Rhc),"/tables/",Table,"/list_keys"]),
    StartRef = make_ref(),
    Pid = spawn(rhc_listkeys, list_acceptor, [self(), StartRef, ts_keys]),
    case request_stream(Pid, get, Url, [], [], Rhc) of
        {ok, ReqId} ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} ->
            {error, Error}
    end.


-spec query(#rhc{}, string()) ->
                   {ok, ts_selection()} | {error, bad_body | {integer(), binary()} | term()}.
query(Rhc, Query) ->
    query(Rhc, Query, []).

-spec query(#rhc{}, string(), proplists:proplist()) ->
                   ts_selection() | {error, bad_body | {integer(), binary()} | term()}.
query(Rhc, Query, Options) ->
    Url = lists:flatten([root_url(Rhc), "/query"]),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
    case request(post, Url, ["200", "204", "401", "409", "400"], Headers, Query, Rhc) of
        {ok, TwoHundred, _Headers, BodyJson}
          when TwoHundred == "200";
               TwoHundred == "204" ->
            case catch mochijson2:decode(BodyJson) of
                {struct, [{<<"success">>, true}]} ->
                    %% idiosyncratic way to report {ok, {[], []}}
                    {ok, {[], []}};
                {struct, [{<<"columns">>, Columns}, {<<"rows">>, Rows}]} ->
                    %% convert records (coming in as lists) to tuples
                    %% to conform to the representation used in riakc
                    {ok, {Columns, [list_to_tuple(R) || R <- Rows]}};
                _Wat ->
                    {error, bad_body}
            end;
        {_, Code, _, Body} when is_list(Code) ->
            {error, {list_to_integer(Code), Body}};
        {error, Error} ->
            {error, Error}
    end.



%% ------------------------
%% supporting functions

%% @doc Get the client ID to use, given the passed options and client.
%%      Choose the client ID in Options before the one in the client.
%% @spec client_id(#rhc{}, proplist()) -> client_id()
client_id(#rhc{options = RhcOptions}, Options) ->
    proplists:get_value(
      client_id, Options,
      proplists:get_value(client_id, RhcOptions)).

%% @doc Generate a random client ID.
%% @spec random_client_id() -> client_id()
random_client_id() ->
    Id = crypto:rand_bytes(32),
    integer_to_list(erlang:phash2(Id)).

%% @doc Assemble the root URL for the given client
%% @spec root_url(#rhc{}) -> iolist()
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
%% @spec options(#rhc{}) -> proplist()
options(#rhc{options=Options}) ->
    Options.

%% @doc Extract the list of query parameters to use for a GET
%% @spec get_q_params(#rhc{}, proplist()) -> proplist()
get_q_params(Rhc, Options) ->
    options_list([timeout],
                 Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a PUT
%% @spec put_q_params(#rhc{}, proplist()) -> proplist()
put_q_params(Rhc, Options) ->
    options_list([],
                 Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a DELETE
%% @spec delete_q_params(#rhc{}, proplist()) -> proplist()
delete_q_params(Rhc, Options) ->
    options_list([timeout],
                 Options ++ options(Rhc)).


-spec options_list([Key::atom()|{Key::atom(),Alias::string()}],
                   proplists:proplist()) -> proplists:proplist().
%% @doc Extract the options for the given `Keys' from the possible
%%      list of `Options'.
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

make_get_url(Rhc, Table, Key, Qs, Options) ->
    make_keys_url(Rhc, Table, Key, Qs, Options).
make_delete_url(Rhc, Table, Key, Qs, Options) ->
    make_keys_url(Rhc, Table, Key, Qs, Options).
make_keys_url(Rhc, Table, Key, Qs, Options) ->
    %% serialize key elements as /f1/v1/f2/v2:
    %% 1. fetch field names, supplied separately in this call's Options
    case proplists:get_value(key_column_names, Options) of
        ColNames when is_list(ColNames),
                      length(ColNames) == length(Key) ->
            EnrichedKey = lists:zip(ColNames, Key),
            {ok, lists:flatten(
                   [root_url(Rhc),
                    "/tables/", Table,
                    keys_to_path_elements_append(EnrichedKey, "/keys"),
                    [[$?, mochiweb_util:urlencode(Qs)] || Qs /= []]])};
        undefined ->
            %% caller didn't specify column names: try v1/v2/v3
            {ok, lists:flatten(
                   [root_url(Rhc),
                    "/tables/", Table,
                    keys_to_path_elements_append(Key, "/keys"),
                    [[$?, mochiweb_util:urlencode(Qs)] || Qs /= []]])};
        _ColNames when is_list(_ColNames) ->
            {error, key_col_count_mismatch};
        _Invalid ->
            {error, invalid_col_names}
    end.

keys_to_path_elements_append([], Acc) ->
    lists:flatten(Acc);
keys_to_path_elements_append([{F, V} | T], Acc) ->
    keys_to_path_elements_append(
      T, lists:append(
           Acc, [$/, maybe_composite_field_to_list(F),
                 $/, mochiweb_util:quote_plus(value_to_list(V))]));
keys_to_path_elements_append([V | T], Acc) ->
    keys_to_path_elements_append(
      T, lists:append(
           Acc, [  %% bare values
                 $/, mochiweb_util:quote_plus(value_to_list(V))])).

maybe_composite_field_to_list(X) when is_binary(X) ->
    %% the user gave us a simplified representation of [<<"a">>]
    binary_to_list(X);
maybe_composite_field_to_list(C = [E|_]) when is_binary(E) ->
    %% full representation
    string:join(
      [binary_to_list(X) || X <- C], ".").


value_to_list(X) when is_binary(X) ->
    binary_to_list(X);
value_to_list(X) when is_integer(X) ->
    integer_to_list(X);
value_to_list(X) when is_float(X) ->
    float_to_list(X).

make_put_url(Rhc, Table, Qs_) ->
    Qs = [{K, iolist_to_binary(V)} || {K,V} <- Qs_],
    {ok, lists:flatten(
           [root_url(Rhc),
            "/tables/", Table, "/keys/",
            [[$?, mochiweb_util:urlencode(Qs)] || Qs /= []]])}.

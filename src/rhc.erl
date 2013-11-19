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

%% @doc Riak Erlang HTTP Client.  This module provides access to Riak's
%%      HTTP interface.  For basic usage, please read
%%      <a href="overview.html">the riakhttpc application overview</a>.
-module(rhc).

-export([create/0, create/4,
         ip/1,
         port/1,
         prefix/1,
         options/1,
         ping/1,
         get_client_id/1,
         get_server_info/1,
         get_server_stats/1,
         get/3, get/4,
         put/2, put/3,
         delete/3, delete/4, delete_obj/2, delete_obj/3,
         list_buckets/1,
         list_buckets/2,
         stream_list_buckets/1,
         stream_list_buckets/2,
         list_keys/2,
         list_keys/3,
         stream_list_keys/2,
         stream_list_keys/3,
         get_index/4, get_index/5,
         stream_index/4, stream_index/5,
         get_bucket/2,
         set_bucket/3,
         reset_bucket/2,
         get_bucket_type/2,
         set_bucket_type/3,
         reset_bucket_type/2,
         mapred/3,mapred/4,
         mapred_stream/4, mapred_stream/5,
         mapred_bucket/3, mapred_bucket/4,
         mapred_bucket_stream/5,
         search/3, search/5,
         counter_incr/4, counter_incr/5,
         counter_val/3, counter_val/4,
         fetch_type/3, fetch_type/4,
         update_type/4, update_type/5,
         modify_type/5
         ]).

-include("raw_http.hrl").
-include("rhc.hrl").

-export_type([rhc/0]).
-opaque rhc() :: #rhc{}.

%% @doc Create a client for connecting to the default port on localhost.
%% @equiv create("127.0.0.1", 8098, "riak", [])
create() ->
    create("127.0.0.1", 8098, "riak", []).

%% @doc Create a client for connecting to a Riak node.
%%
%%      Connections are made to:
%%      ```http://IP:Port/Prefix/(<bucket>/<key>)'''
%%
%%      Defaults for r, w, dw, rw, and return_body may be passed in
%%      the Options list.  The client id can also be specified by
%%      adding `{client_id, ID}' to the Options list.
%% @spec create(string(), integer(), string(), Options::list()) -> rhc()
create(IP, Port, Prefix, Opts0) when is_list(IP), is_integer(Port),
                                     is_list(Prefix), is_list(Opts0) ->
    Opts = case proplists:lookup(client_id, Opts0) of
               none -> [{client_id, random_client_id()}|Opts0];
               Bin when is_binary(Bin) ->
                   [{client_id, binary_to_list(Bin)}
                    | [ O || O={K,_} <- Opts0, K =/= client_id ]];
               _ ->
                   Opts0
           end,
    #rhc{ip=IP, port=Port, prefix=Prefix, options=Opts}.

%% @doc Get the IP this client will connect to.
%% @spec ip(rhc()) -> string()
ip(#rhc{ip=IP}) -> IP.

%% @doc Get the Port this client will connect to.
%% @spec port(rhc()) -> integer()
port(#rhc{port=Port}) -> Port.

%% @doc Get the prefix this client will use for object URLs
%% @spec prefix(rhc()) -> string()
prefix(#rhc{prefix=Prefix}) -> Prefix.

%% @doc Ping the server by requesting the "/ping" resource.
%% @spec ping(rhc()) -> ok|{error, term()}
ping(Rhc) ->
    Url = ping_url(Rhc),
    case request(get, Url, ["200","204"], [], [], Rhc) of
        {ok, _Status, _Headers, _Body} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

%% @doc Get the client ID that this client will use when storing objects.
%% @spec get_client_id(rhc()) -> {ok, string()}
get_client_id(Rhc) ->
    {ok, client_id(Rhc, [])}.

%% @doc Get some basic information about the server.  The proplist returned
%%      should include `node' and `server_version' entries.
%% @spec get_server_info(rhc()) -> {ok, proplist()}|{error, term()}
get_server_info(Rhc) ->
    Url = stats_url(Rhc),
    case request(get, Url, ["200"], [], [], Rhc) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_server_info(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Get the list of full stats from a /stats call to the server.
%% @spec get_server_info(rhc()) -> {ok, proplist()}|{error, term()}
get_server_stats(Rhc) ->
    Url = stats_url(Rhc),
    case request(get, Url, ["200"], [], [], Rhc) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            Stats = lists:flatten(Response),
            {ok, Stats};
        {error, Error} ->
            {error, Error}
    end.

%% @equiv get(Rhc, Bucket, Key, [])
get(Rhc, Bucket, Key) ->
    get(Rhc, Bucket, Key, []).

%% @doc Get the objects stored under the given bucket and key.
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`r'</dt>
%%          <dd>The 'R' value to use for the read</dd>
%%        <dt>`timeout'</dt>
%%          <dd>The server-side timeout for the write in ms</dd>
%%      </dl>
%%
%%      The term in the second position of the error tuple will be
%%      `notfound' if the key was not found.
%% @spec get(rhc(), bucket(), key(), proplist())
%%          -> {ok, riakc_obj()}|{error, term()}
get(Rhc, Bucket, Key, Options) ->
    Qs = get_q_params(Rhc, Options),
    Url = make_url(Rhc, Bucket, Key, Qs),
    case request(get, Url, ["200", "300"], [], [], Rhc) of
        {ok, _Status, Headers, Body} ->
            {ok, rhc_obj:make_riakc_obj(Bucket, Key, Headers, Body)};
        {error, {ok, "404", Headers, _}} ->
            case proplists:get_value(deletedvclock, Options) of
                true ->
                    case proplists:get_value("X-Riak-Vclock", Headers) of
                        undefined ->
                            {error, notfound};
                        Vclock ->
                            {error, {notfound, base64:decode(Vclock)}}
                    end;
                _ ->
                    {error, notfound}
            end;
        {error, Error} ->
            {error, Error}
    end.

%% @equiv put(Rhc, Object, [])
put(Rhc, Object) ->
    put(Rhc, Object, []).

%% @doc Store the given object in Riak.
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`w'</dt>
%%          <dd>The 'W' value to use for the write</dd>
%%        <dt>`dw'</dt>
%%          <dd>The 'DW' value to use for the write</dd>
%%        <dt>`timeout'</dt>
%%          <dd>The server-side timeout for the write in ms</dd>
%%        <dt>return_body</dt>
%%          <dd>Whether or not to return the updated object in the
%%          response.  `ok' is returned if return_body is false.
%%          `{ok, Object}' is returned if return_body is true.</dd>
%%      </dl>
%% @spec put(rhc(), riakc_obj(), proplist())
%%         -> ok|{ok, riakc_obj()}|{error, term()}
put(Rhc, Object, Options) ->
    Qs = put_q_params(Rhc, Options),
    Bucket = riakc_obj:bucket(Object),
    Key = riakc_obj:key(Object),
    Url = make_url(Rhc, Bucket, Key, Qs),
    Method = if Key =:= undefined -> post;
                true              -> put
             end,
    {Headers0, Body} = rhc_obj:serialize_riakc_obj(Rhc, Object),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}
               |Headers0],
    case request(Method, Url, ["200", "204", "300"], Headers, Body, Rhc) of
        {ok, Status, ReplyHeaders, ReplyBody} ->
            if Status =:= "204" ->
                    ok;
               true ->
                    {ok, rhc_obj:make_riakc_obj(Bucket, Key,
                                                ReplyHeaders, ReplyBody)}
            end;
        {error, Error} ->
            {error, Error}
    end.

%% @doc Increment the counter stored under `bucket', `key'
%%      by the given `amount'.
%% @equiv counter_incr(rhc(), binary(), binary(), integer(), []).
-spec counter_incr(rhc(), binary(), binary(), integer()) -> ok | {ok, integer()}
                                                                | {error, term()}.
counter_incr(Rhc, Bucket, Key, Amt) ->
    counter_incr(Rhc, Bucket, Key, Amt, []).

%% @doc Increment the counter stored at `bucket', `key' by
%%      `Amt'. Note: `Amt' can be a negative or positive integer.
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`w'</dt>
%%          <dd>The 'W' value to use for the write</dd>
%%        <dt>`dw'</dt>
%%          <dd>The 'DW' value to use for the write</dd
%%        <dt>`pw'</dt>
%%          <dd>The 'PW' value to use for the write</dd>
%%        <dt>`timeout'</dt>
%%          <dd>The server-side timeout for the write in ms</dd>
%%        <dt>`returnvalue'</dt>
%%          <dd>Whether or not to return the updated value in the
%%          response. `ok' is returned if returnvalue is absent | `false'.
%%          `{ok, integer()}' is returned if returnvalue is `true'.</dd>
%%      </dl>
%% @see the riak docs at http://docs.basho.com/riak/latest/references/apis/http/ for details
-spec counter_incr(rhc(), binary(), binary(), integer(), list()) -> ok | {ok, integer()}
                                                                        | {error, term()}.
counter_incr(Rhc, Bucket, Key, Amt, Options) ->
    Qs = counter_q_params(Rhc, Options),
    Url = make_counter_url(Rhc, Bucket, Key, Qs),
    Body = integer_to_list(Amt),
    case request(post, Url, ["200", "204"], [], Body, Rhc) of
        {ok, Status, _ReplyHeaders, ReplyBody} ->
            if Status =:= "204" ->
                    ok;
               true ->
                    {ok, list_to_integer(binary_to_list(ReplyBody))}
            end;
        {error, Error} ->
            {error, Error}
    end.

%% @doc Get the counter stored at `bucket', `key'.
-spec counter_val(rhc(), term(), term()) -> {ok, integer()} | {error, term()}.
counter_val(Rhc, Bucket, Key) ->
    counter_val(Rhc, Bucket, Key, []).

%% @doc Get the counter stored at `bucket', `key'.
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`r'</dt>
%%          <dd>The 'R' value to use for the read</dd>
%%        <dt>`pr'</dt>
%%          <dd>The 'PR' value to use for the read</dd
%%        <dt>`pw'</dt>
%%          <dd>The 'PW' value to use for the write</dd>
%%        <dt>`timeout'</dt>
%%          <dd>The server-side timeout for the write in ms</dd>
%%        <dt>`notfound_ok'</dt>
%%          <dd>if `true' not_found replies from vnodes count toward read quorum.<//dd>
%%        <dt>`basic_quorum'</dt>
%%          <dd>When set to `true' riak will return a value as soon as it gets a quorum of responses.</dd>
%%      </dl>
%% @see the riak docs at http://docs.basho.com/riak/latest/references/apis/http/ fro details
-spec counter_val(rhc(), term(), term(), list()) -> {ok, integer()} | {error, term()}.
counter_val(Rhc, Bucket, Key, Options) ->
    Qs = counter_q_params(Rhc, Options),
    Url = make_counter_url(Rhc, Bucket, Key, Qs),
    case request(get, Url, ["200"], [], [], Rhc) of
        {ok, "200", _ReplyHeaders, ReplyBody} ->
            {ok, list_to_integer(binary_to_list(ReplyBody))};
        {error, Error} ->
            {error, Error}
    end.

%% @equiv delete(Rhc, Bucket, Key, [])
delete(Rhc, Bucket, Key) ->
    delete(Rhc, Bucket, Key, []).

%% @doc Delete the given key from the given bucket.
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`rw'</dt>
%%          <dd>The 'RW' value to use for the delete</dd>
%%        <dt>`timeout'</dt>
%%          <dd>The server-side timeout for the write in ms</dd>
%%      </dl>
%% @spec delete(rhc(), bucket(), key(), proplist()) -> ok|{error, term()}
delete(Rhc, Bucket, Key, Options) ->
    Qs = delete_q_params(Rhc, Options),
    Url = make_url(Rhc, Bucket, Key, Qs),
    Headers0 = case lists:keyfind(vclock, 1, Options) of
                   false -> [];
                   {vclock, V} ->
                       [{?HEAD_VCLOCK, base64:encode_to_string(V)}]
               end,
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}|Headers0],
    case request(delete, Url, ["204"], Headers, [], Rhc) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.


%% @equiv delete_obj(Rhc, Obj, [])
delete_obj(Rhc, Obj) ->
    delete_obj(Rhc, Obj, []).

%% @doc Delete the key of the given object, using the contained vector
%% clock if present.
%% @equiv delete(Rhc, riakc_obj:bucket(Obj), riakc_obj:key(Obj), [{vclock, riakc_obj:vclock(Obj)}|Options])
delete_obj(Rhc, Obj, Options) ->
    Bucket = riakc_obj:bucket(Obj),
    Key = riakc_obj:key(Obj),
    VClock = riakc_obj:vclock(Obj),
    delete(Rhc, Bucket, Key, [{vclock, VClock}|Options]).

list_buckets(Rhc) ->
    list_buckets(Rhc, undefined).

list_buckets(Rhc, BucketType) when is_binary(BucketType) ->
    list_buckets(Rhc, BucketType, undefined);
list_buckets(Rhc, Timeout) ->
    list_buckets(Rhc, undefined, Timeout).

list_buckets(Rhc, BucketType, Timeout) ->
    {ok, ReqId} = stream_list_buckets(Rhc, BucketType, Timeout),
    rhc_listkeys:wait_for_list(ReqId, Timeout).

stream_list_buckets(Rhc) ->
    stream_list_buckets(Rhc, undefined).

stream_list_buckets(Rhc, BucketType) when is_binary(BucketType) ->
    stream_list_buckets(Rhc, BucketType, undefined);
stream_list_buckets(Rhc, Timeout) ->
    stream_list_buckets(Rhc, undefined, Timeout).

stream_list_buckets(Rhc, BucketType, Timeout) ->
    ParamList0 = [{?Q_BUCKETS, ?Q_STREAM},
                  {?Q_PROPS, ?Q_FALSE}],
    ParamList =
        case Timeout of
            undefined -> ParamList0;
            N -> ParamList0 ++ [{?Q_TIMEOUT, N}]
        end,
    Url = make_url(Rhc, {BucketType, undefined}, undefined, ParamList),
    StartRef = make_ref(),
    Pid = spawn(rhc_listkeys, list_acceptor, [self(), StartRef, buckets]),
    case request_stream(Pid, get, Url, [], [], Rhc) of
        {ok, ReqId}    ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} -> {error, Error}
    end.

list_keys(Rhc, Bucket) ->
    list_keys(Rhc, Bucket, undefined).

%% @doc List the keys in the given bucket.
%% @spec list_keys(rhc(), bucket()) -> {ok, [key()]}|{error, term()}

list_keys(Rhc, Bucket, Timeout) ->
    {ok, ReqId} = stream_list_keys(Rhc, Bucket, Timeout),
    rhc_listkeys:wait_for_list(ReqId, ?DEFAULT_TIMEOUT).

stream_list_keys(Rhc, Bucket) ->
    stream_list_keys(Rhc, Bucket, undefined).

%% @doc Stream key lists to a Pid.  Messages sent to the Pid will
%%      be of the form `{reference(), message()}'
%%      where `message()' is one of:
%%      <dl>
%%         <dt>`done'</dt>
%%            <dd>end of key list, no more messages will be sent</dd>
%%         <dt>`{keys, [key()]}'</dt>
%%            <dd>a portion of the key list</dd>
%%         <dt>`{error, term()}'</dt>
%%            <dd>an error occurred</dd>
%%      </dl>
%% @spec stream_list_keys(rhc(), bucket()) ->
%%          {ok, reference()}|{error, term()}
stream_list_keys(Rhc, Bucket, Timeout) ->
    ParamList0 = [{?Q_KEYS, ?Q_STREAM},
                  {?Q_PROPS, ?Q_FALSE}],
    ParamList =
        case Timeout of
            undefined -> ParamList0;
            N -> ParamList0 ++ [{?Q_TIMEOUT, N}]
        end,
    Url = make_url(Rhc, Bucket, undefined, ParamList),
    StartRef = make_ref(),
    Pid = spawn(rhc_listkeys, list_acceptor, [self(), StartRef, keys]),
    case request_stream(Pid, get, Url, [], [], Rhc) of
        {ok, ReqId}    ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} -> {error, Error}
    end.

%% @doc Query a secondary index.
%% @spec get_index(rhc(), bucket(), index(), index_query()) ->
%%    {ok, index_results()} | {error, term()}
get_index(Rhc, Bucket, Index, Query) ->
    get_index(Rhc, Bucket, Index, Query, []).

%% @doc Query a secondary index.
%% @spec get_index(rhc(), bucket(), index(), index_query(), index_options()) ->
%%    {ok, index_results()} | {error, term()}
get_index(Rhc, Bucket, Index, Query, Options) ->
    {ok, ReqId} = stream_index(Rhc, Bucket, Index, Query, Options),
    rhc_index:wait_for_index(ReqId).

%% @doc Query a secondary index, streaming the results back.
%% @spec stream_index(rhc(), bucket(), index(), index_query()) ->
%%    {ok, reference()} | {error, term()}
stream_index(Rhc, Bucket, Index, Query) ->
    stream_index(Rhc, Bucket, Index, Query, []).

%% @doc Query a secondary index, streaming the results back.
%% @spec stream_index(rhc(), bucket(), index(), index_query(), index_options()) ->
%%    {ok, reference()} | {error, term()}
stream_index(Rhc, Bucket, Index, Query, Options) ->
    ParamList = rhc_index:query_options([{stream, true}|Options]),
    Url = index_url(Rhc, Bucket, Index, Query, ParamList),
    StartRef = make_ref(),
    Pid = spawn(rhc_index, index_acceptor, [self(), StartRef]),
    case request_stream(Pid, get, Url, [], [], Rhc) of
        {ok, ReqId} ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Get the properties of the given bucket.
%% @spec get_bucket(rhc(), bucket()) -> {ok, proplist()}|{error, term()}
get_bucket(Rhc, Bucket) ->
    Url = make_url(Rhc, Bucket, undefined, [{?Q_PROPS, ?Q_TRUE},
                                            {?Q_KEYS, ?Q_FALSE}]),
    case request(get, Url, ["200"], [], [], Rhc) of
        {ok, "200", _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {struct, Props} = proplists:get_value(?JSON_PROPS, Response),
            {ok, rhc_bucket:erlify_props(Props)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Set the properties of the given bucket.
%%
%%      Allowed properties are:
%%      <dl>
%%        <dt>`n_val'</dt>
%%          <dd>The 'N' value to use for storing data in this bucket</dd>
%%        <dt>`allow_mult'</dt>
%%          <dd>Whether or not this bucket should allow siblings to
%%          be created for its keys</dd>
%%      </dl>
%% @spec set_bucket(rhc(), bucket(), proplist()) -> ok|{error, term()}
set_bucket(Rhc, Bucket, Props0) ->
    Url = make_url(Rhc, Bucket, undefined, [{?Q_PROPS, ?Q_TRUE}]),
    Headers =  [{"Content-Type", "application/json"}],
    Props = rhc_bucket:httpify_props(Props0),
    Body = mochijson2:encode({struct, [{?Q_PROPS, {struct, Props}}]}),
    case request(put, Url, ["204"], Headers, Body, Rhc) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.

reset_bucket(Rhc, Bucket) ->
    Url = make_url(Rhc, Bucket, undefined, [{?Q_PROPS, ?Q_TRUE}]),
    case request(delete, Url, ["204"], [], [], Rhc) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.


%% @doc Get the properties of the given bucket.
%% @spec get_bucket(rhc(), bucket()) -> {ok, proplist()}|{error, term()}
get_bucket_type(Rhc, Type) ->
    Url = make_url(Rhc, {Type, undefined}, undefined, [{?Q_PROPS, ?Q_TRUE},
                                            {?Q_KEYS, ?Q_FALSE}]),
    case request(get, Url, ["200"], [], [], Rhc) of
        {ok, "200", _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {struct, Props} = proplists:get_value(?JSON_PROPS, Response),
            {ok, rhc_bucket:erlify_props(Props)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Set the properties of the given bucket type.
%%
%% @spec set_bucket(rhc(), bucket(), proplist()) -> ok|{error, term()}
set_bucket_type(Rhc, Type, Props0) ->
    Url = make_url(Rhc, {Type, undefined}, undefined, [{?Q_PROPS, ?Q_TRUE}]),
    Headers =  [{"Content-Type", "application/json"}],
    Props = rhc_bucket:httpify_props(Props0),
    Body = mochijson2:encode({struct, [{?Q_PROPS, {struct, Props}}]}),
    case request(put, Url, ["204"], Headers, Body, Rhc) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.

reset_bucket_type(Rhc, Type) ->
    Url = make_url(Rhc, {Type, undefined}, undefined, [{?Q_PROPS, ?Q_TRUE}]),
    case request(delete, Url, ["204"], [], [], Rhc) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.

%% @equiv mapred(Rhc, Inputs, Query, DEFAULT_TIMEOUT)
mapred(Rhc, Inputs, Query) ->
    mapred(Rhc, Inputs, Query, ?DEFAULT_TIMEOUT).

%% @doc Execute a map/reduce query. See {@link
%%      rhc_mapred:encode_mapred/2} for details of the allowed formats
%%      for `Inputs' and `Query'.
%% @spec mapred(rhc(), rhc_mapred:map_input(),
%%              [rhc_mapred:query_part()], integer())
%%         -> {ok, [rhc_mapred:phase_result()]}|{error, term()}
mapred(Rhc, Inputs, Query, Timeout) ->
    {ok, ReqId} = mapred_stream(Rhc, Inputs, Query, self(), Timeout),
    rhc_mapred:wait_for_mapred(ReqId, Timeout).

%% @equiv mapred_stream(Rhc, Inputs, Query, ClientPid, DEFAULT_TIMEOUT)
mapred_stream(Rhc, Inputs, Query, ClientPid) ->
    mapred_stream(Rhc, Inputs, Query, ClientPid, ?DEFAULT_TIMEOUT).

%% @doc Stream map/reduce results to a Pid.  Messages sent to the Pid
%%      will be of the form `{reference(), message()}',
%%      where `message()' is one of:
%%      <dl>
%%         <dt>`done'</dt>
%%            <dd>query has completed, no more messages will be sent</dd>
%%         <dt>`{mapred, integer(), mochijson()}'</dt>
%%            <dd>partial results of a query the second item in the tuple
%%             is the (zero-indexed) phase number, and the third is the
%%             JSON-decoded results</dd>
%%         <dt>`{error, term()}'</dt>
%%             <dd>an error occurred</dd>
%%      </dl>
%% @spec mapred_stream(rhc(), rhc_mapred:mapred_input(),
%%                     [rhc_mapred:query_phase()], pid(), integer())
%%          -> {ok, reference()}|{error, term()}
mapred_stream(Rhc, Inputs, Query, ClientPid, Timeout) ->
    Url = mapred_url(Rhc),
    StartRef = make_ref(),
    Pid = spawn(rhc_mapred, mapred_acceptor, [ClientPid, StartRef, Timeout]),
    Headers = [{?HEAD_CTYPE, "application/json"}],
    Body = rhc_mapred:encode_mapred(Inputs, Query),
    case request_stream(Pid, post, Url, Headers, Body, Rhc) of
        {ok, ReqId} ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} -> {error, Error}
    end.

%% @doc Execute a search query. This command will return an error
%%      unless executed against a Riak Search cluster.
%% @spec search(rhc(), bucket(), string()) ->
%%       {ok, [rhc_mapred:phase_result()]}|{error, term()}
search(Rhc, Bucket, SearchQuery) ->
    %% Run a Map/Reduce operation using reduce_identity to get a list
    %% of BKeys.
    IdentityQuery = [{reduce, {modfun, riak_kv_mapreduce, reduce_identity}, none, true}],
    case search(Rhc, Bucket, SearchQuery, IdentityQuery, ?DEFAULT_TIMEOUT) of
        {ok, [{_, Results}]} ->
            %% Unwrap the results.
            {ok, Results};
        Other -> Other
    end.

%% @doc Execute a search query and feed the results into a map/reduce
%%      query. See {@link rhc_mapred:encode_mapred/2} for details of
%%      the allowed formats for `MRQuery'. This command will return an error
%%      unless executed against a Riak Search cluster.
%% @spec search(rhc(), bucket(), string(),
%%       [rhc_mapred:query_part()], integer()) ->
%%       {ok, [rhc_mapred:phase_result()]}|{error, term()}
search(Rhc, Bucket, SearchQuery, MRQuery, Timeout) ->
    Inputs = {modfun, riak_search, mapred_search, [Bucket, SearchQuery]},
    mapred(Rhc, Inputs, MRQuery, Timeout).

%% @equiv mapred_bucket(Rhc, Bucket, Query, DEFAULT_TIMEOUT)
mapred_bucket(Rhc, Bucket, Query) ->
    mapred_bucket(Rhc, Bucket, Query, ?DEFAULT_TIMEOUT).

%% @doc Execute a map/reduce query over all keys in the given bucket.
%% @spec mapred_bucket(rhc(), bucket(), [rhc_mapred:query_phase()],
%%                     integer())
%%          -> {ok, [rhc_mapred:phase_result()]}|{error, term()}
mapred_bucket(Rhc, Bucket, Query, Timeout) ->
    {ok, ReqId} = mapred_bucket_stream(Rhc, Bucket, Query, self(), Timeout),
    rhc_mapred:wait_for_mapred(ReqId, Timeout).

%% @doc Stream map/reduce results over all keys in a bucket to a Pid.
%%      Similar to {@link mapred_stream/5}
%% @spec mapred_bucket_stream(rhc(), bucket(),
%%                     [rhc_mapred:query_phase()], pid(), integer())
%%          -> {ok, reference()}|{error, term()}
mapred_bucket_stream(Rhc, Bucket, Query, ClientPid, Timeout) ->
    mapred_stream(Rhc, Bucket, Query, ClientPid, Timeout).

%% @doc Fetches the representation of a convergent datatype from Riak.
-spec fetch_type(rhc(), {BucketType::binary(), Bucket::binary()}, Key::binary()) ->
                        {ok, riakc_datatype:datatype()} | {error, term()}.
fetch_type(Rhc, BucketAndType, Key) ->
    fetch_type(Rhc, BucketAndType, Key, []).

%% @doc Fetches the representation of a convergent datatype from Riak,
%% using the given request options.
-spec fetch_type(rhc(), {BucketType::binary(), Bucket::binary()}, Key::binary(), [proplists:property()]) ->
                        {ok, riakc_datatype:datatype()} | {error, term()}.
fetch_type(Rhc, BucketAndType, Key, Options) ->
    Query = fetch_type_q_params(Rhc, Options),
    Url = make_datatype_url(Rhc, BucketAndType, Key, Query),
    case request(get, Url, ["200"], [], [], Rhc) of
        {ok, _Status, _Headers, Body} ->
            {ok, rhc_dt:datatype_from_json(mochijson2:decode(Body))};
        {error, Reason} ->
            {error, rhc_dt:decode_error(fetch, Reason)}
    end.

%% @doc Updates the convergent datatype in Riak with local
%% modifications stored in the container type.
-spec update_type(rhc(), {BucketType::binary(), Bucket::binary()}, Key::binary(),
                  Update::riakc_datatype:update(term())) ->
                         ok | {ok, Key::binary()} | {ok, riakc_datatype:datatype()} |
                         {ok, Key::binary(), riakc_datatype:datatype()} | {error, term()}.
update_type(Rhc, BucketAndType, Key, Update) ->
    update_type(Rhc, BucketAndType, Key, Update, []).

-spec update_type(rhc(), {BucketType::binary(), Bucket::binary()}, Key::binary(),
                  Update::riakc_datatype:update(term()), [proplists:property()]) ->
                         ok | {ok, Key::binary()} | {ok, riakc_datatype:datatype()} |
                         {ok, Key::binary(), riakc_datatype:datatype()} | {error, term()}.
update_type(_Rhc, _BucketAndType, _Key, undefined, _Options) ->
    {error, unmodified};
update_type(Rhc, BucketAndType, Key, {Type, Op, Context}, Options) ->
    Query = update_type_q_params(Rhc, Options),
    Url = make_datatype_url(Rhc, BucketAndType, Key, Query),
    Body = mochijson2:encode(rhc_dt:encode_update_request(Type, Op, Context)),
    case request(post, Url, ["200", "201", "204"],
                 [{"Content-Type", "application/json"}], Body, Rhc) of
        {ok, "204", _H, _B} ->
            %% not creation, no returnbody
            ok;
        {ok, "200", _H, Body} ->
            %% returnbody was specified
            {ok, rhc_dt:datatype_from_json(mochijson2:decode(Body))};
        {ok, "201", Headers, Body} ->
            %% Riak-assigned key
            Url = proplists:get_value("Location", Headers),
            Key = list_to_binary(lists:last(string:tokens(Url, "/"))),
            case proplists:get_value("Content-Length", Headers) of
                "0" ->
                    {ok, Key};
                _ ->
                    {ok, Key, rhc_dt:datatype_from_json(mochijson2:decode(Body))}
            end;
        {error, Reason} ->
            {error, rhc_dt:decode_error(update, Reason)}
    end.

%% @doc Fetches, applies the given function to the value, and then
%% updates the datatype in Riak. If an existing value is not found,
%% but you want the updates to apply anyway, use the 'create' option.
-spec modify_type(rhc(), fun((riakc_datatype:datatype()) -> riakc_datatype:datatype()),
                  {BucketType::binary(), Bucket::binary()}, Key::binary(), [proplists:property()]) ->
                         ok | {ok, riakc_datatype:datatype()} | {error, term()}.
modify_type(Rhc, Fun, BucketAndType, Key, Options) ->
    Create = proplists:get_value(create, Options, true),
    case fetch_type(Rhc, BucketAndType, Key, Options) of
        {ok, Data} ->
            NewData = Fun(Data),
            Mod = riakc_datatype:module_for_term(NewData),
            update_type(Rhc, BucketAndType, Key, Mod:to_op(NewData), Options);
        {error, {notfound, Type}} when Create ->
            %% Not found, but ok to create it
            Mod = riakc_datatype:module(Type),
            NewData = Fun(Mod:new()),
            update_type(Rhc, BucketAndType, Key, Mod:to_op(NewData), Options);
        {error, Reason} ->
            {error, Reason}
    end.


%% INTERNAL

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
    {{Y,Mo,D},{H,Mi,S}} = erlang:universaltime(),
    {_,_,NowPart} = now(),
    Id = erlang:phash2([Y,Mo,D,H,Mi,S,node(),NowPart]),
    base64:encode_to_string(<<Id:32>>).

%% @doc Assemble the root URL for the given client
%% @spec root_url(rhc()) -> iolist()
root_url(#rhc{ip=Ip, port=Port, options=Opts}) ->
    Proto = case proplists:get_value(is_ssl, Opts) of
        true ->
            "https";
        _ ->
            "http"
    end,
    [Proto, "://",Ip,":",integer_to_list(Port),"/"].

%% @doc Assemble the URL for the map/reduce resource
%% @spec mapred_url(rhc()) -> iolist()
mapred_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "mapred/?chunked=true"])).

%% @doc Assemble the URL for the ping resource
%% @spec ping_url(rhc()) -> iolist()
ping_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "ping/"])).

%% @doc Assemble the URL for the stats resource
%% @spec stats_url(rhc()) -> iolist()
stats_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "stats/"])).

%% @doc Assemble the URL for the 2I resource
index_url(Rhc, BucketAndType, Index, Query, Params) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    QuerySegments = index_query_segments(Query),
    IndexName = index_name(Index),
    unicode:characters_to_list(
      [root_url(Rhc),
       [ ["types", "/", Type, "/"] || Type =/= undefined ],
       "buckets", "/", Bucket, "/", "index", "/", IndexName,
       [ [ "/", QS] || QS <- QuerySegments ],
       [ ["?", mochiweb_util:urlencode(Params)] || Params =/= []]]).


index_query_segments(B) when is_binary(B) ->
    [ B ];
index_query_segments(I) when is_integer(I) ->
    [ integer_to_list(I) ];
index_query_segments({B1, B2}) when is_binary(B1),
                                    is_binary(B2)->
    [ B1, B2 ];
index_query_segments({I1, I2}) when is_integer(I1),
                                    is_integer(I2) ->
    [ integer_to_list(I1), integer_to_list(I2) ];
index_query_segments(_) -> [].

index_name({binary_index, B}) ->
    [B, "_bin"];
index_name({integer_index, I}) ->
    [I, "_int"];
index_name(Idx) -> Idx.



%% @doc Assemble the URL for the given bucket and key
%% @spec make_url(rhc(), bucket(), key(), proplist()) -> iolist()
make_url(Rhc=#rhc{}, BucketAndType, Key, Query) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    {IsKeys, IsProps, IsBuckets} = detect_bucket_flags(Query),
    unicode:characters_to_list(
        [root_url(Rhc),
         [ [ "types", "/", Type, "/"] || Type =/= undefined ],
         %% Prefix, "/",
         [ [ "buckets" ] || IsBuckets ],
         [ ["buckets", "/", Bucket,"/"] || Bucket =/= undefined ],
         [ [ "keys" ] || IsKeys ],
         [ [ "props" ] || IsProps ],
         [ ["keys", "/", Key,"/"] || Key =/= undefined andalso not IsKeys andalso not IsProps ],
         [ ["?", mochiweb_util:urlencode(Query)] || Query =/= [] ]
        ]).

%% @doc Generate a counter url.
-spec make_counter_url(rhc(), term(), term(), list()) -> iolist().
make_counter_url(Rhc, Bucket, Key, Query) ->
    binary_to_list(
      iolist_to_binary(
        [root_url(Rhc),
         <<"buckets">>, "/", Bucket, "/", <<"counters">>, "/", Key, "?",
         [ [mochiweb_util:urlencode(Query)] || Query =/= []]])).

make_datatype_url(Rhc, BucketAndType, Key, Query) ->
    case extract_bucket_type(BucketAndType) of
        {undefined, _B} ->
            throw(default_bucket_type_disallowed);
        {Type, Bucket} ->
            unicode:characters_to_list(
              [root_url(Rhc),
               "types/", Type,
               "/buckets/", Bucket,
               "/datatypes/", [ Key || Key /= undefined ],
               [ ["?", mochiweb_util:urlencode(Query)] || Query /= [] ]])
    end.

%% @doc send an ibrowse request
request(Method, Url, Expect, Headers, Body, Rhc) ->
    AuthHeader = get_auth_header(Rhc#rhc.options),
    SSLOptions = get_ssl_options(Rhc#rhc.options),
    Accept = {"Accept", "multipart/mixed, */*;q=0.9"},
    case ibrowse:send_req(Url, [Accept|Headers] ++ AuthHeader, Method, Body,
                          [{response_format, binary}] ++ SSLOptions) of
        Resp={ok, Status, _, _} ->
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
    case ibrowse:send_req(Url, Headers ++ AuthHeader, Method, Body,
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
    options_list([r,pr,timeout], Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a PUT
%% @spec put_q_params(rhc(), proplist()) -> proplist()
put_q_params(Rhc, Options) ->
    options_list([r,w,dw,pr,pw,timeout,asis,{return_body,"returnbody"}],
                 Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a
%% counter increment
-spec counter_q_params(rhc(), list()) -> list().
counter_q_params(Rhc, Options) ->
    options_list([r, pr, w, pw, dw, returnvalue, basic_quorum, notfound_ok], Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a DELETE
%% @spec delete_q_params(rhc(), proplist()) -> proplist()
delete_q_params(Rhc, Options) ->
    options_list([r,w,dw,pr,pw,rw,timeout], Options ++ options(Rhc)).

fetch_type_q_params(Rhc, Options) ->
    options_list([r,pr,basic_quorum,notfound_ok,timeout,include_context], Options ++ options(Rhc)).

update_type_q_params(Rhc, Options) ->
    options_list([r,w,dw,pr,pw,basic_quorum,notfound_ok,timeout,include_context,{return_body, "returnbody"}],
                 Options ++ options(Rhc)).

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

%% @doc Convert a stats-resource response to an erlang-term server
%%      information proplist.
erlify_server_info(Props) ->
    lists:flatten([ erlify_server_info(K, V) || {K, V} <- Props ]).
erlify_server_info(<<"nodename">>, Name) -> {node, Name};
erlify_server_info(<<"riak_kv_version">>, Vsn) -> {server_version, Vsn};
erlify_server_info(_Ignore, _) -> [].

get_auth_header(Options) ->
    case lists:keyfind(credentials, 1, Options) of
        {credentials, User, Password} ->
            [{"Authorization", "Basic " ++ base64:encode_to_string(User ++ ":"
                                                                  ++
                                                                  Password)}];
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

extract_bucket_type({<<"default">>, B}) ->
    {undefined, B};
extract_bucket_type({T,B}) ->
    {T,B};
extract_bucket_type(B) ->
    {undefined, B}.

detect_bucket_flags(Query) ->
    {proplists:get_value(?Q_KEYS, Query, ?Q_FALSE) =/= ?Q_FALSE,
     proplists:get_value(?Q_PROPS, Query, ?Q_FALSE) =/= ?Q_FALSE,
     proplists:get_value(?Q_BUCKETS, Query, ?Q_FALSE) =/= ?Q_FALSE}.

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
         fetch/2, fetch/3, push/3,
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
         mapred/3,mapred/4,
         mapred_stream/4, mapred_stream/5,
         mapred_bucket/3, mapred_bucket/4,
         mapred_bucket_stream/5,
         search/3, search/5,
         counter_incr/4, counter_incr/5,
         counter_val/3, counter_val/4,
         fetch_type/3, fetch_type/4,
         update_type/4, update_type/5,
         modify_type/5,
         get_preflist/3,
         rt_enqueue/3,
         rt_enqueue/4,
         aae_merge_root/2,
         aae_merge_branches/3,
         aae_fetch_clocks/3,
         aae_fetch_clocks/4,
         aae_range_tree/7,
         aae_range_clocks/5,
         aae_range_replkeys/5,
         aae_find_keys/5,
         aae_find_tombs/5,
         aae_reap_tombs/6,
         aae_erase_keys/6,
         aae_object_stats/4,
         aae_list_buckets/1,
         aae_list_buckets/2
         ]).

-include("raw_http.hrl").
-include("rhc.hrl").
-include_lib("riakc/include/riakc.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([rhc/0]).
-opaque rhc() :: #rhc{}.

-define(DEFAULT_TLS_OPTIONS, [{server_name_indication, disable}]).

-type key_range() :: {riakc_obj:key(), riakc_obj:key()} | all.
-type segment_filter() :: {list(pos_integer()), tree_size()} | all.
-type modified_range() :: {ts(), ts()} | all.
-type ts() :: pos_integer().
-type hash_method() :: pre_hash | {rehash, non_neg_integer()}.
-type change_method() :: {job, pos_integer()}|local|count.

-type tree_size() :: xxsmall| xsmall| small| medium| large| xlarge.

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
    Opts = 
        case proplists:lookup(client_id, Opts0) of
            none ->
                [{client_id, random_client_id()}|Opts0];
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
%% @spec get_server_stats(rhc()) -> {ok, proplist()}|{error, term()}
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

%% @doc Fetch replicated objects from a queue
-spec fetch(rhc(), binary()) ->
                {ok, queue_empty}|
                {error, term()}|
                {ok|crc_wonky,
                    {deleted, term(), binary()}|binary()}.
fetch(Rhc, QueueName) ->
    QParams = [{object_format, internal}],
    URL = fetch_url(Rhc, QueueName, QParams),
    case request(get, URL, ["200"], [], [], Rhc) of
        {ok, _Status, _Headers, Body} ->
            case Body of
                <<0:8/integer>> ->
                    {ok, queue_empty};
                <<1:8/integer, 1:8/integer,
                    TCL:32/integer, TombClockBin:TCL/binary,
                    CRC:32/integer, ObjBin/binary>> ->
                    {crc_check(CRC, ObjBin),
                        {deleted, binary_to_term(TombClockBin), ObjBin}};
                <<1:8/integer, 0:8/integer, CRC:32/integer, ObjBin/binary>> ->
                    {crc_check(CRC, ObjBin), ObjBin}
            end;
        {error, Error} ->
            {error, Error}
    end.

-spec fetch(rhc(), binary(), internal|internal_aaehash) ->
                {ok, queue_empty}|
                {error, term()}|
                {ok|crc_wonky,
                    {deleted, term(), binary()}|
                        binary()|
                        {deleted, term(), binary(), non_neg_integer(), non_neg_integer()}|
                        {binary(), non_neg_integer(), non_neg_integer()}}.
fetch(Rhc, QueueName, internal) ->
    fetch(Rhc, QueueName);
fetch(Rhc, QueueName, internal_aaehash) ->
    QParams = [{object_format, internal_aaehash}],
    URL = fetch_url(Rhc, QueueName, QParams),
    case request(get, URL, ["200"], [], [], Rhc) of
        {ok, _Status, _Headers, Body} ->
            case Body of
                <<0:8/integer>> ->
                    {ok, queue_empty};
                <<1:8/integer, 1:8/integer,
                    SegmentID:32/integer, SegmentHash:32/integer,
                    TCL:32/integer, TombClockBin:TCL/binary,
                    CRC:32/integer, ObjBin/binary>> ->
                    {crc_check(CRC, ObjBin),
                        {deleted,
                            binary_to_term(TombClockBin),
                            ObjBin,
                            SegmentID, SegmentHash}};
                <<1:8/integer, 0:8/integer,
                    SegmentID:32/integer, SegmentHash:32/integer,
                    CRC:32/integer, ObjBin/binary>> ->
                    {crc_check(CRC, ObjBin),
                        {ObjBin, SegmentID, SegmentHash}}
            end;
        {error, Error} ->
            {error, Error}
    end.

-spec push(rhc(),
            binary(),
            [{riakc_obj:bucket(), riakc_obj:key(), riakc_obj:vclock()}]) ->
                {error, term()}|{ok, iolist()}.
push(Rhc, QueueName, KeyClockList) ->
    URL = push_url(Rhc, QueueName),
    ReqHeaders = [{"content-type", "application/json"}],
    ReqBody = encode_keys_and_clocks(KeyClockList),
    case request(post, URL, ["200"], ReqHeaders, ReqBody, Rhc) of
        {ok, "200", _RspHeaders, RspBody} ->
            {ok, RspBody};
        {error, Error} ->
            {error, Error}
    end.


fetch_url(Rhc, QueueName, Params) ->
    binary_to_list(iolist_to_binary([root_url(Rhc),
                                        "queuename/",
                                        mochiweb_util:quote_plus(QueueName),
                                        "?",
                                        mochiweb_util:urlencode(Params)])).

push_url(Rhc, QueueName) ->
    binary_to_list(
        iolist_to_binary(
            [root_url(Rhc),
                "queuename/",
                mochiweb_util:quote_plus(QueueName)])).

-spec encode_keys_and_clocks([{riakc_obj:bucket(), riakc_obj:key(), riakc_obj:vclock()}]) -> iolist().
encode_keys_and_clocks(KeysNClocks) ->
    Keys = {struct, [{<<"keys-clocks">>,
                      [{struct, encode_key_and_clock(Bucket, Key, Clock)} || {Bucket, Key, Clock} <- KeysNClocks]
                     }]},
    mochijson2:encode(Keys).

encode_key_and_clock({Type, Bucket}, Key, Clock) ->
    [{<<"bucket-type">>, Type},
     {<<"bucket">>, Bucket},
     {<<"key">>, Key},
     {<<"clock">>, Clock}];
encode_key_and_clock(Bucket, Key, Clock) ->
    [{<<"bucket">>, Bucket},
     {<<"key">>, Key},
     {<<"clock">>, Clock}].

crc_check(CRC, Bin) ->
    case erlang:crc32(Bin) of
        CRC -> ok;
        _ -> crc_wonky
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

%% @equiv rt_enqueue(Rhc, Bucket, Key, [])
rt_enqueue(Rhc, Bucket, Key) ->
    rt_enqueue(Rhc, Bucket, Key, []).

%% @doc Get the object stored under the given bucket and key, and put
%% it on the realtime repl queue
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`r'</dt>
%%          <dd>The 'R' value to use for the read</dd>
%%        <dt>`pr'</dt>
%%          <dd>The 'PR' value to use for the read</dd>
%%        <dt>`timeout'</dt>
%%          <dd>The server-side timeout for the operation, in ms</dd>
%%      </dl>
%%
%%      The term in the second position of the error tuple will be
%%      `notfound' if the key was not found. It will be
%%      `realtime_not_enabled' if realtime repl is not enabled.
%% @spec rt_enqueue(rhc(), bucket(), key(), proplist())
%%          -> {ok, riakc_obj()}|{error, term()}
rt_enqueue(Rhc, Bucket, Key, Options) ->
    Qs = get_q_params(Rhc, Options),
    Url = make_rtenqueue_url(Rhc, Bucket, Key, Qs),
    case request(post, Url, ["204"], [], [], Rhc) of
        {ok, _Status, _Headers, _Body} ->
            ok;
        {error, {ok, "404", _Headers, _}} ->
            {error, notfound};
        {error, {ok, "500", _Header_, <<"Error:\nrealtime_not_enabled\n">>}} ->
                {error, realtime_not_enabled};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Get the merged aae tictactree root for the given `NVal'
-spec aae_merge_root(rhc(), NVal::pos_integer()) ->
                            {ok, {root, binary()}} |
                            {error, any()}.
aae_merge_root(Rhc, NVal) ->
    Url = make_cached_aae_url(Rhc, root, NVal, undefined),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_aae_root(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc get the aae merged branches for the given `NVal', restricted
%% to the given list of `Branches'
-spec aae_merge_branches(rhc(),
                         NVal::pos_integer(),
                         Branches::list(pos_integer())) ->
                                         {ok, {branches, [{BranchId::integer(), Branch::binary()}]}} |
                                         {error, any()}.
aae_merge_branches(Rhc, NVal, Branches) ->
    Url = make_cached_aae_url(Rhc, branch, NVal, Branches),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_aae_branches(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc fetch the keys and clocks for the given `NVal', restricted
%% to the given list of `Segments'
-spec aae_fetch_clocks(rhc(),
                       NVal::pos_integer(),
                       Segments::list(pos_integer())) ->
                              {ok, {keysclocks, [{{riakc_obj:bucket(), riakc_obj:key()}, binary()}]}} |
                              {error, any()}.
aae_fetch_clocks(Rhc, NVal, Segments) ->
    Url = make_cached_aae_url(Rhc, keysclocks, NVal, Segments),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_aae_keysclocks(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc fetch the keys and clocks for the given `NVal', restricted
%% to the given list of `Segments' and by a modified range
-spec aae_fetch_clocks(rhc(),
                       NVal::pos_integer(),
                       Segments::list(pos_integer()),
                       ModifiedRange::modified_range()) ->
                              {ok, {keysclocks, [{{riakc_obj:bucket(), riakc_obj:key()}, binary()}]}} |
                              {error, any()}.
aae_fetch_clocks(Rhc, NVal, Segments, ModifiedRange) ->
    Url =
        make_cached_aae_url(Rhc, keysclocks, NVal, {Segments, ModifiedRange}),
    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_aae_keysclocks(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc generate a tictac tree by folding over a range of keys
%% in`Bucket'. The fold can be limited to the keys in `KeyRange' which
%% is a pair `{Start::binary(), End::binary()}` that defines a range
%% of keys, or the atom `all'. The `TreeSize' parameter is an atom,
%% one of `xxsmall', `xsmall', `small', `medium', `large', or `xlarge'
%% which determines, well, the tictac tree size. `SegmentFilter'
%% further limits ths returned tree, it can be a pair of `{Segments,
%% TreeSize}' where `Segments' is a list of integers (segments to
%% return) and `TreeSize' the tree size that was initially queried to
%% return the segments in `Segments', or it can be the atom
%% `all'. `ModifiedRange' can restrict the tree fold to only include
%% keys whose last modified date is in the range. The Range is a pair
%% `{Start::pos_integer(), End::pos_integer()}' where both `Start' and
%% `End' are 32-bit unix timestamps that represents seconds since the
%% epoch. Finally `HashMethod' is one of `pre_hash' or `{rehash,
%% IV::non_neg_integer()}'. The former uses the default hashing, the
%% latter instructs the tictac tree to be built hashing the objects'
%% vector clocks with a hash initialised with the value of `IV'. This
%% is for those of you worried about hash collisions.  NOTE: what is
%% returned is mochijson2 style {struct, ETC} terms, as this is what
%% leveled_tictact:import_tree expects
-spec aae_range_tree(rhc(), riakc_obj:bucket(),
                     key_range(), tree_size(),
                     segment_filter(), modified_range(), hash_method()) ->
                            {ok, {tree, Tree::any()}} | {error, any()}.
aae_range_tree(Rhc, BucketAndType, KeyRange,
                TreeSize, SegmentFilter, ModifiedRange, HashMethod) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    Url =
        lists:flatten(
          [root_url(Rhc),
           "rangetrees", "/", %% the AAE-Fold range fold prefix
           [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
           "buckets", "/", mochiweb_util:quote_plus(Bucket),"/"
           "trees", "/", atom_to_list(TreeSize),
           "?filter=", encode_aae_range_filter(KeyRange, SegmentFilter, ModifiedRange, HashMethod)
          ]),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_aae_tree(Response)};
        {error, Error} ->
            {error, Error}
    end.

-spec aae_range_clocks(rhc(), riakc_obj:bucket(), key_range(), segment_filter(), modified_range()) ->
                              {ok, {keysclocks, [{{riakc_obj:bucket(), riakc_obj:key()}, binary()}]}} |
                              {error, any()}.
aae_range_clocks(Rhc, BucketAndType, KeyRange, SegmentFilter, ModifiedRange) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    Url =
        lists:flatten(
          [root_url(Rhc),
           "rangetrees", "/", %% the AAE-Fold range fold prefix
           [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
           "buckets", "/", mochiweb_util:quote_plus(Bucket),"/"
           "keysclocks",
           "?filter=", encode_aae_range_filter(KeyRange, SegmentFilter, ModifiedRange, undefined)
          ]),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_aae_keysclocks(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc aae_range_repllkeys
%% Fold over a range of keys and queue up those keys to be replicated to the
%% other site.  Once the keys are replicated the objects will then be fetched,
%% as long as a site is consuming from that replication queue.
%% Will return the number of keys which have been queued for replication.
-spec aae_range_replkeys(rhc(), riakc_obj:bucket(),
                            key_range(), modified_range(),
                            atom()) ->
                                {ok, non_neg_integer()} |
                                    {error, any()}.
aae_range_replkeys(Rhc, BucketType, KeyRange, ModifiedRange, QueueName) ->
    {Type, Bucket} = extract_bucket_type(BucketType),
    Url =
        lists:flatten(
          [root_url(Rhc),
           "rangerepl", "/", %% the AAE-Fold range fold prefix
           [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"]
                || Type =/= undefined ],
           "buckets", "/", mochiweb_util:quote_plus(Bucket), "/",
           "queuename", "/", mochiweb_util:quote_plus(QueueName),
           "?filter=",
            encode_aae_range_filter(KeyRange, all, ModifiedRange, undefined)
          ]),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            [{<<"dispatched_count">>, DispatchedCount}] = Response,
            {ok, DispatchedCount};
        {error, Error} ->
            {error, Error}
    end.

%% @doc aae_find_keys folds over the tictacaae store to get
%% operational information. `Rhc' is the client. `Bucket' is the
%% bucket to fold over. `KeyRange' as before is a two tuple of
%% `{Start, End}' where both ` Start' and `End' are binaries that
%% represent the first and last key of a range to fold over. The atom
%% `all' means all fol over keys in the bucket. `ModifiedRange' is a
%% pair `{StartDate, EndDate}' or 32-bit integer unix timestamps, or
%% the atom `all', that limits the fold to only the keys that have a
%% last-modified date in the range. the `Query' is either
%% `{sibling_coun, N}` or `{object_size, N}' where `N' is an
%% integer. for `sibling_count' `N' means return all keys that have
%% more than `N' siblings. NOTE: 1 sibling means a single value in
%% this implementation, therefore if you want all keys that have more
%% than a single value AT THE VNODE then `{sibling_count, 1}' is your
%% query. NOTE NOTE: It is possible that all N vnodes have a single
%% value, and that value is different on each vnode (temporarily
%% only), this query would not detect that state. For `object_size' it
%% means return all keys whose object size is greater than `N'. The
%% result is a list of pairs `{Key, Count | Size}'
-spec aae_find_keys(rhc(), riakc_obj:bucket(), key_range(), modified_range(), Query) ->
                           {ok, {keys, list({riakc_obj:key(), pos_integer()})}} |
                           {error, any()} when
      Query :: {sibling_count, pos_integer()} | {object_size, pos_integer()}.
aae_find_keys(Rhc, BucketAndType, KeyRange, ModifiedRange, Query) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    {Prefix, Suffix} =
        case element(1, Query) of
            sibling_count -> {"siblings", "counts"};
            object_size -> {"objectsizes", "sizes"}
        end,
    Url =
        lists:flatten(
          [root_url(Rhc),
           Prefix, "/",
           [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
           "buckets", "/", mochiweb_util:quote_plus(Bucket),"/",
           Suffix, "/",
           integer_to_list(element(2, Query)),
           "?filter=", encode_aae_find_keys_filter(KeyRange, undefined, ModifiedRange)
          ]),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_aae_find_keys(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc find_tombs will find tombstone keys in a given bucket and key_range
%% returning the key and delete_hash, where the delete_hash is an integer that
%% can be used in a reap request. The SegmentFilter is intended to be used as a
%% mechanism for assiting in scheduling work - a way for splitting out the
%% process of finding/reaping tombstones into batches without having
%% inconsistencies within the AAE trees.
-spec aae_find_tombs(rhc(),
                    riakc_obj:bucket(), key_range(),
                    segment_filter(),
                    modified_range()) ->
                        {ok, {keys, list({riakc_obj:key(), pos_integer()})}} |
                        {error, any()}.
aae_find_tombs(Rhc, BucketAndType, KeyRange, SegmentFilter, ModifiedRange) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    Url =
        lists:flatten(
          [root_url(Rhc),
            "tombs", "/", %% the AAE-Fold range fold prefix
            [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
            "buckets", "/", mochiweb_util:quote_plus(Bucket),"/",
            "?filter=",
                encode_aae_find_keys_filter(KeyRange,
                                                SegmentFilter,
                                                ModifiedRange)
          ]),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_aae_find_keys(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc reap_tombs will find tombstone keys in a given bucket and key_range.
%% The SegmentFilter is intended to be used as a mechanism for assiting in
%% scheduling work - a way for splitting out the process of finding/reaping
%% tombstones into batches without having inconsistencies within the AAE trees.
%% reap_tombs can be passed a change_method of count if a count of matching
%% tombstones is all that is required - this is an alternative to running
%% find_tombs and taking the length of the list.  To actually reap either
%% `local` of `{ob, ID}` should be passed as the change_method.  Using `local`
%% will reap each tombstone from the node local to which it is discovered,
%% whch will have the impact of distributing the reap load across the cluster
%% and increasing parallelisation of reap activity.  Otherwise a job id can be
%% passed an a specific reaper will be started on the co-ordinating node of the
%% query only.  The Id will be a positive integer used to identify this reap
%% task in logs. 
-spec aae_reap_tombs(rhc(),
                    riakc_obj:bucket(), key_range(),
                    segment_filter(),
                    modified_range(),
                    change_method()) ->
                        {ok, non_neg_integer()} | {error, any()}.
aae_reap_tombs(Rhc,
                BucketAndType, KeyRange,
                SegmentFilter, ModifiedRange,
                ChangeMethod) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    Url =
        lists:flatten(
          [root_url(Rhc),
           "reap", "/",
           [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
           "buckets", "/", mochiweb_util:quote_plus(Bucket),"/",
           "?filter=",
                encode_aae_action_filter(KeyRange,
                                            SegmentFilter,
                                            ModifiedRange,
                                            ChangeMethod)
          ]),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            [{<<"dispatched_count">>, DispatchedCount}] = Response,
            {ok, DispatchedCount};
        {error, Error} ->
            {error, Error}
    end.

%% @doc erase_keys will find keys in a given bucket and key_range.
%% The SegmentFilter is intended to be used as a mechanism for assiting in
%% scheduling work - a way for splitting out the process of finding/reaping
%% tombstones into batches without having inconsistencies within the AAE trees.
%% erase_keys can be passed a change_method of count if a count of matching
%% keys is all that is required - this is an alternative to running
%% find_keys and taking the length of the list.  To actually erase the object
%% either `local` of `{ob, ID}` should be passed as the change_method.  Using
%% `local` will delete each object from the node local to which it is
%% discovered, which will have the impact of distributing the delete load
%% across the cluster and increasing parallelisation of delete activity. 
%% Otherwise a job id can be passed an a specific eraser process will be
%% started on the co-ordinating node of the query only.  The Id will be a
%% positive integer used to identify this erase task in logs. 
-spec aae_erase_keys(rhc(),
                    riakc_obj:bucket(), key_range(),
                    segment_filter(),
                    modified_range(),
                    change_method()) ->
                        {ok, non_neg_integer()} | {error, any()}.
aae_erase_keys(Rhc,
                BucketAndType, KeyRange,
                SegmentFilter, ModifiedRange,
                ChangeMethod) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    Url =
        lists:flatten(
          [root_url(Rhc),
           "erase", "/",
           [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
           "buckets", "/", mochiweb_util:quote_plus(Bucket),"/",
           "?filter=",
                encode_aae_action_filter(KeyRange,
                                            SegmentFilter,
                                            ModifiedRange,
                                            ChangeMethod)
          ]),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            [{<<"dispatched_count">>, DispatchedCount}] = Response,
            {ok, DispatchedCount};
        {error, Error} ->
            {error, Error}
    end.

%% @doc aae_object_stats folds over the tictacaae store to get
%% operational information. `Rhc' is the client. `Bucket' is the
%% bucket to fold over. `KeyRange' as before is a two tuple of
%% `{Start, End}' where both ` Start' and `End' are binaries that
%% represent the first and last key of a range to fold over. The atom
%% `all' means all fol over keys in the bucket. `ModifiedRange' is a
%% pair `{StartDate, EndDate}' or 32-bit integer unix timestamps, or
%% the atom `all', that limits the fold to only the keys that have a
%% last-modified date in the range. the `Query' is either
%% `{sibling_coun, N}` or `{object_size, N}' where `N' is an
%% integer. for `sibling_count' `N' means return all keys that have
%% more than `N' siblings. NOTE: 1 sibling means a single value in
%% this implementation, therefore if you want all keys that have more
%% than a single value AT THE VNODE then `{sibling_count, 1}' is your
%% query. NOTE NOTE: It is possible that all N vnodes have a single
%% value, and that value is different on each vnode (temporarily
%% only), this query would not detect that state. For `object_size' it
%% means return all keys whose object size is greater than `N'. The
%% result is a list of pairs `{Key, Count | Size}'
-spec aae_object_stats(rhc(), riakc_obj:bucket(), key_range(), modified_range()) ->
                           {ok, {stats, list({Key::atom(), Val::atom() | list()})}} |
                           {error, any()}.
aae_object_stats(Rhc, BucketAndType, KeyRange, ModifiedRange) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    Url =
        lists:flatten(
          [root_url(Rhc),
          "objectstats/",
           [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
           "buckets", "/", mochiweb_util:quote_plus(Bucket),
           "?filter=", encode_aae_find_keys_filter(KeyRange, undefined, ModifiedRange)
          ]),

    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, {stats, erlify_aae_object_stats(Response)}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc
%% List all the buckets with references in the AAE store.  For reasonable 
%% (e.g. < o(1000)) this should be quick and efficient unless using the
%% leveled_so parallel store.  A minimum n_val can be passed if known.  If
%% there are buckets (with keys) below the minimum n_val they may not be
%% detecting in the query.  Will default to 1.
-spec aae_list_buckets(rhc()) -> {ok, list(riakc_obj:bucket())}.
aae_list_buckets(Rhc) ->
    Url = lists:flatten([root_url(Rhc), "aaebucketlist"]),
    aae_list_buckets(Rhc, Url).

-spec aae_list_buckets(rhc(), pos_integer()|string())
                                            -> {ok, list(riakc_obj:bucket())}.
aae_list_buckets(Rhc, MinNVal) when is_integer(MinNVal), MinNVal > 0 ->
    Url = lists:flatten([root_url(Rhc), "aaebucketlist",
                            "?filter=", integer_to_list(MinNVal)]),
    aae_list_buckets(Rhc, Url);
aae_list_buckets(Rhc, Url) when is_list(Url) ->
    case request(get, Url, ["200"], [], [], Rhc, ?AAEFOLD_TIMEOUT) of
        {ok, _Status, _Headers, Body} ->
            {struct, [{<<"results">>, Response}]} = mochijson2:decode(Body),
            {ok, erlify_aae_buckets(Response)};
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
%% @equiv counter_incr(Rhc, Bucket, Key, Amt, [])
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
%%          <dd>The 'DW' value to use for the write</dd>
%%        <dt>`pw'</dt>
%%          <dd>The 'PW' value to use for the write</dd>
%%        <dt>`timeout'</dt>
%%          <dd>The server-side timeout for the write in ms</dd>
%%        <dt>`returnvalue'</dt>
%%          <dd>Whether or not to return the updated value in the
%%          response. `ok' is returned if returnvalue is absent | `false'.
%%          `{ok, integer()}' is returned if returnvalue is `true'.</dd>
%%      </dl>
%% See the riak docs at http://docs.basho.com/riak/latest/references/apis/http/ for details
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
%%          <dd>The 'PR' value to use for the read</dd>
%%        <dt>`pw'</dt>
%%          <dd>The 'PW' value to use for the write</dd>
%%        <dt>`timeout'</dt>
%%          <dd>The server-side timeout for the write in ms</dd>
%%        <dt>`notfound_ok'</dt>
%%          <dd>if `true' not_found replies from vnodes count toward read quorum.</dd>
%%        <dt>`basic_quorum'</dt>
%%          <dd>When set to `true' riak will return a value as soon as it gets a quorum of responses.</dd>
%%        <dt>`node_confirms'</dt>
%%          <dd>The number of separate nodes which host vnodes that need to be consulted as part of the fetch.</dd>
%%      </dl>
%% See the riak docs at http://docs.basho.com/riak/latest/references/apis/http/ for details
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
%% @spec list_keys(rhc(), bucket(), integer()) -> {ok, [key()]}|{error, term()}

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
%% @spec stream_list_keys(rhc(), bucket(), integer()) ->
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
%% @spec get_bucket_type (rhc(), bucket()) -> {ok, proplist()}|{error, term()}
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
%% @spec set_bucket_type(rhc(), bucket(), proplist()) -> ok|{error, term()}
set_bucket_type(Rhc, Type, Props0) ->
    Url = make_url(Rhc, {Type, undefined}, undefined, [{?Q_PROPS, ?Q_TRUE}]),
    Headers =  [{"Content-Type", "application/json"}],
    Props = rhc_bucket:httpify_props(Props0),
    Body = mochijson2:encode({struct, [{?Q_PROPS, {struct, Props}}]}),
    case request(put, Url, ["204"], Headers, Body, Rhc) of
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
        {ok, "200", _H, RBody} ->
            %% returnbody was specified
            {ok, rhc_dt:datatype_from_json(mochijson2:decode(RBody))};
        {ok, "201", Headers, RBody} ->
            %% Riak-assigned key
            Url = proplists:get_value("Location", Headers),
            Key = unicode:characters_to_binary(lists:last(string:tokens(Url, "/")), utf8),
            case proplists:get_value("Content-Length", Headers) of
                "0" ->
                    {ok, Key};
                _ ->
                    {ok, Key, rhc_dt:datatype_from_json(mochijson2:decode(RBody))}
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
            Mod = riakc_datatype:module_for_type(Type),
            NewData = Fun(Mod:new()),
            update_type(Rhc, BucketAndType, Key, Mod:to_op(NewData), Options);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get the active preflist based on a particular bucket/key
%%      combination.
-spec get_preflist(rhc(), binary(), binary()) -> {ok, [tuple()]}|{error, term()}.
get_preflist(Rhc, Bucket, Key) ->
    Url = make_preflist_url(Rhc, Bucket, Key),
    case request(get, Url, ["200"], [], [], Rhc) of
        {ok, "200", _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_preflist(Response)};
        {error, Error} ->
            {error, Error}
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
    {_,_,NowPart} = os:timestamp(),
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
    lists:flatten(
      [root_url(Rhc),
       [ ["types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
       "buckets", "/", mochiweb_util:quote_plus(Bucket), "/", "index", "/", IndexName,
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
    lists:flatten(
        [root_url(Rhc),
         [ [ "types", "/", mochiweb_util:quote_plus(Type), "/"] || Type =/= undefined ],
         %% Prefix, "/",
         [ [ "buckets" ] || IsBuckets ],
         [ ["buckets", "/", mochiweb_util:quote_plus(Bucket),"/"] || Bucket =/= undefined ],
         [ [ "keys" ] || IsKeys ],
         [ [ "props" ] || IsProps ],
         [ ["keys", "/", mochiweb_util:quote_plus(Key), "/"] ||
           Key =/= undefined andalso not IsKeys andalso not IsProps ],
         [ ["?", mochiweb_util:urlencode(Query)] || Query =/= [] ]
        ]).

%% @doc Generate a preflist url.
-spec make_preflist_url(rhc(), binary(), binary()) -> iolist().
make_preflist_url(Rhc, BucketAndType, Key) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    lists:flatten(
      [root_url(Rhc),
       [ [ "types", "/", Type, "/"] || Type =/= undefined ],
       [ ["buckets", "/", Bucket,"/"] || Bucket =/= undefined ],
       [ [ "keys", "/", Key,"/" ] || Key =/= undefined],
       [ ["preflist/"] ]]).

%% @private create the RTEnqueue URL
make_rtenqueue_url(Rhc=#rhc{}, BucketAndType, Key, Query) ->
    {Type, Bucket} = extract_bucket_type(BucketAndType),
    lists:flatten(
        [root_url(Rhc),
         "rtq", %% THE RTENEQUEUE URL prefix
         [ ["/", mochiweb_util:quote_plus(Type)] || Type =/= undefined ],
         [ ["/", mochiweb_util:quote_plus(Bucket)] || Bucket =/= undefined ],
         [ ["/", mochiweb_util:quote_plus(Key)] || Key =/= undefined],
         [ ["?", mochiweb_util:urlencode(Query)] || Query =/= [] ]
        ]).

-spec make_cached_aae_url(rhc(),
                          root | branch | keysclocks,
                          NVal :: pos_integer(),
                          IDs :: list(non_neg_integer())|
                                    undefined|
                                    {list(non_neg_integer()),
                                        modified_range()}) ->
                                 iolist().
make_cached_aae_url(Rhc, Type, NVal, undefined) ->
    complete_cached_aae_url(Rhc, NVal, Type, []);
make_cached_aae_url(Rhc, Type, NVal, IDs) when is_list(IDs) ->
    Filter = ["?filter=", encode_aae_cached_filter(IDs)],
    complete_cached_aae_url(Rhc, NVal, Type, Filter);
make_cached_aae_url(Rhc, Type, NVal, {IDs, ModifiedRange}) ->
    Filter = ["?filter=", encode_aae_cached_filter(IDs, ModifiedRange)],
    complete_cached_aae_url(Rhc, NVal, Type, Filter).


complete_cached_aae_url(Rhc, NVal, Type, Filter) ->
    lists:flatten(
      [root_url(Rhc),
       "cachedtrees", "/", %% the AAE-Fold cachedtrees prefix
       "nvals", "/",
       integer_to_list(NVal), "/",
       atom_to_list(Type),
       Filter
      ]).



%% @doc this is a list of integers. Segment IDs or Branches, but
%% either way, just json encode a list of ints
-spec encode_aae_cached_filter(list(pos_integer())) -> string().
encode_aae_cached_filter(Filter) ->
    JSON = mochijson2:encode(Filter),
    base64:encode_to_string(lists:flatten(JSON)).

-spec encode_aae_cached_filter(list(pos_integer()),
                                modified_range()) -> string().
encode_aae_cached_filter(SegmentIDs, ModifiedRange) ->
    FilterElems = [EncodeFun(FilterElem) || {EncodeFun, FilterElem} <-
                                  [{fun encode_segment_filter/1,
                                      {SegmentIDs, large}},
                                   {fun encode_modified_range/1,
                                       ModifiedRange}]],
    JSON = mochijson2:encode({struct, lists:flatten(FilterElems)}),
    base64:encode_to_string(iolist_to_binary(JSON)).

-spec encode_aae_find_keys_filter(key_range(),
                                    segment_filter() | undefined,
                                    modified_range()) ->
                                        string().
encode_aae_find_keys_filter(KeyRange, SegmentFilter, ModifiedRange) ->
    FilterElems = [EncodeFun(FilterElem) || {EncodeFun, FilterElem} <-
                                [{fun encode_key_range/1, KeyRange},
                                    {fun encode_segment_filter/1, SegmentFilter},
                                    {fun encode_modified_range/1, ModifiedRange}]
                  ],
    JSON = mochijson2:encode({struct, lists:flatten(FilterElems)}),
    base64:encode_to_string(iolist_to_binary(JSON)).

%% @private create a base64 encoded JSON string used by aae_range_* API
-spec encode_aae_range_filter(key_range(), segment_filter(), modified_range(), undefined | hash_method()) ->
                                     string().
encode_aae_range_filter(KeyRange, SegmentFilter, ModifiedRange, HashMethod) ->
    FilterElems = [EncodeFun(FilterElem) || {EncodeFun, FilterElem} <-
                                  [{fun encode_key_range/1, KeyRange},
                                   {fun encode_segment_filter/1, SegmentFilter},
                                   {fun encode_modified_range/1, ModifiedRange},
                                   {fun encode_hash_method/1, HashMethod}]],
    JSON = mochijson2:encode({struct, lists:flatten(FilterElems)}),
    base64:encode_to_string(iolist_to_binary(JSON)).

-spec encode_aae_action_filter(key_range(),
                                segment_filter(),
                                modified_range(), 
                                change_method()) ->
                                     string().
encode_aae_action_filter(KeyRange, SegmentFilter, ModifiedRange, ChangeMethod) ->
    FilterElems = [EncodeFun(FilterElem) || {EncodeFun, FilterElem} <-
                                  [{fun encode_key_range/1, KeyRange},
                                   {fun encode_segment_filter/1, SegmentFilter},
                                   {fun encode_modified_range/1, ModifiedRange},
                                   {fun encode_change_method/1, ChangeMethod}]],
    JSON = mochijson2:encode({struct, lists:flatten(FilterElems)}),
    base64:encode_to_string(iolist_to_binary(JSON)).


-spec encode_key_range(all | {binary(), binary()}) ->
                              [] | {binary(), {struct, list()}}.
encode_key_range(all) ->
    [];
encode_key_range({Start, End}) when is_binary(Start), is_binary(End) ->
    {<<"key_range">>, {struct, [{<<"start">>, Start},
                                {<<"end">>, End}]}}.

-spec encode_segment_filter(all | {list(pos_integer()), tree_size()}) ->
                                   [] | {binary(), {struct, list()}}.
encode_segment_filter(all) ->
    [];
encode_segment_filter(undefined) ->
    [];
encode_segment_filter({SegList, TreeSize}) when is_list(SegList), is_atom(TreeSize) ->
    {<<"segment_filter">>, {struct, [{<<"segments">>, SegList},
                                     {<<"tree_size">>, atom_to_list(TreeSize)}]}}.

-spec encode_modified_range(all | {pos_integer(), pos_integer()}) ->
                                   [] | {binary(), {struct, list()}}.
encode_modified_range(all) ->
    [];
encode_modified_range({Start, End}) when is_integer(Start), is_integer(End) ->
    {<<"date_range">>, {struct, [{<<"start">>, Start},
                                 {<<"end">>, End}]}}.

-spec encode_hash_method(undefined | pre_hash | {rehash, pos_integer()}) ->
                                [] | {binary(), pos_integer()}.
encode_hash_method({rehash, IV}) when is_integer(IV) ->
    {<<"hash_iv">>, IV};
encode_hash_method(_) ->
    [].

-spec encode_change_method(undefined | change_method()) ->
                    [] | {binary(), binary()} | {binary(), {struct, list()}}.
encode_change_method(count) ->
    {<<"change_method">>, <<"count">>};
encode_change_method(local) ->
    {<<"change_method">>, <<"local">>};
encode_change_method({job, JobID}) ->
    {<<"change_method">>, {struct, [{<<"job_id">>, JobID}]}};
encode_change_method(_) ->
    [].


%% @doc Generate a counter url.
-spec make_counter_url(rhc(), term(), term(), list()) -> iolist().
make_counter_url(Rhc, Bucket, Key, Query) ->
    lists:flatten(
      [root_url(Rhc),
       <<"buckets">>, "/", mochiweb_util:quote_plus(Bucket), "/",
       <<"counters">>, "/", mochiweb_util:quote_plus(Key), "?",
       [ [mochiweb_util:urlencode(Query)] || Query =/= []]]).

make_datatype_url(Rhc, BucketAndType, Key, Query) ->
    case extract_bucket_type(BucketAndType) of
        {undefined, _B} ->
            throw(default_bucket_type_disallowed);
        {Type, Bucket} ->
            lists:flatten(
              [root_url(Rhc),
               "types/", mochiweb_util:quote_plus(Type),
               "/buckets/", mochiweb_util:quote_plus(Bucket),
               "/datatypes/", [ mochiweb_util:quote_plus(Key) || Key /= undefined ],
               [ ["?", mochiweb_util:urlencode(Query)] || Query /= [] ]])
    end.

%% @doc send an ibrowse request
request(Method, Url, Expect, Headers, Body, Rhc) ->
    request(Method, Url, Expect, Headers, Body, Rhc, ?DEFAULT_TIMEOUT).

request(Method, Url, Expect, Headers, Body, Rhc, Timeout) ->
    AuthHeader = get_auth_header(Rhc#rhc.options),
    SSLOptions = get_ssl_options(Rhc#rhc.options),
    Accept = {"Accept", "multipart/mixed, */*;q=0.9"},
    case ibrowse:send_req(Url, [Accept|Headers] ++ AuthHeader, Method, Body,
                          [{response_format, binary}] ++ SSLOptions,
                          Timeout) of
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
    options_list([r,pr,notfound_ok,node_confirms,timeout], Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a PUT
%% @spec put_q_params(rhc(), proplist()) -> proplist()
put_q_params(Rhc, Options) ->
    options_list([r,w,dw,pr,pw,timeout,asis,node_confirms,{return_body,"returnbody"}],
                 Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a
%% counter increment
-spec counter_q_params(rhc(), list()) -> list().
counter_q_params(Rhc, Options) ->
    options_list([r, pr, w, pw, dw, returnvalue, basic_quorum, node_confirms, notfound_ok], Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a DELETE
%% @spec delete_q_params(rhc(), proplist()) -> proplist()
delete_q_params(Rhc, Options) ->
    options_list([r,w,dw,pr,pw,rw,timeout], Options ++ options(Rhc)).

fetch_type_q_params(Rhc, Options) ->
    options_list([r,pr,basic_quorum,node_confirms,notfound_ok,timeout,include_context], Options ++ options(Rhc)).

update_type_q_params(Rhc, Options) ->
    options_list([r,w,dw,pr,pw,basic_quorum, node_confirms,
                  notfound_ok,timeout,include_context,{return_body, "returnbody"}],
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

%% @private convert an aae range tree response to erlang terms. NOTE:
%% what is returned is mochijson2 style {struct, ETC} terms, as this
%% is what leveled_tictact:import_tree expects
-spec erlify_aae_tree([{Key::binary(), Value::list()}]) -> {tree, any()}.
erlify_aae_tree([{<<"tree">>, Tree}]) ->
    {tree, Tree}.


%% @doc convert the aae fold root response to an erlang term
-spec erlify_aae_root([{Key::binary(), Value::string()}]) ->
                             {root, binary()}.
erlify_aae_root([{<<"root">>, Base64Root}]) ->
    {root, base64:decode(Base64Root)}.

%% @doc convert the aae fold branches response to an erlang term
-spec erlify_aae_branches([{Key::binary(), Value::list()}]) ->
                             {branches, [{BranchId::integer(), Branch::binary()}]}.
erlify_aae_branches([{<<"branches">>, Branches}]) ->
    DecodedBranches = [erlify_aae_branch(Branch) || Branch <- Branches],
    {branches, DecodedBranches}.

-spec erlify_aae_branch({struct, list(any())}) ->
                               {integer(), binary()}.
erlify_aae_branch({struct, Props}) ->
    BranchId = proplists:get_value(<<"branch-id">>, Props),
    BranchBase64 = proplists:get_value(<<"branch">>, Props),
    {BranchId, base64:decode(BranchBase64)}.

-spec erlify_aae_object_stats(list()) -> list().
erlify_aae_object_stats(Stats) ->
    lists:foldl(fun({K, V}, Acc) ->
                        case V of
                            I when is_integer(I) ->
                                [{K, I} | Acc];
                            {struct, L} ->
                                InnerStats = erlify_aae_object_stats(L),
                                [{K, InnerStats} | Acc];
                            [] ->
                                Acc
                        end
                end,
                [],
                Stats).

-spec erlify_aae_find_keys([{Key::binary(), Val::non_neg_integer() | list(any())}]) ->
                                  {keys, list({riakc_obj:key(), non_neg_integer()})}.
erlify_aae_find_keys([{<<"results">>, Keys}]) ->
    {keys, [erlify_aae_find_key(KV) || KV <- Keys]}.

-spec erlify_aae_find_key({struct, list()}) ->
                                 {riakc_obj:bucket(), pos_integer()}.
erlify_aae_find_key({struct, Props}) ->
    Key = proplists:get_value(<<"key">>, Props),
    Val = proplists:get_value(<<"value">>, Props),
    {Key, Val}.

%% @doc convert the aae fold branches response to an erlang term
-spec erlify_aae_keysclocks([{Key::binary(), Value::string()}]) ->
                             {keysclocks,
                              [{{binary() | {binary(), binary()}, binary()}, OpaqueVclock::binary()}]
                             }.
erlify_aae_keysclocks([{<<"keys-clocks">>, KeysClocks}]) ->
    DecodedClocks = [erlify_aae_keyclock(KC) || KC <- KeysClocks],
    {keysclocks, DecodedClocks}.

-spec erlify_aae_keyclock({struct, proplists:proplist()}) ->
                               {{binary() | {binary(), binary()}, binary()}, binary()}.
erlify_aae_keyclock({struct, Props}) ->
    BType = proplists:get_value(<<"bucket-type">>, Props, <<"default">>),
    Bucket = proplists:get_value(<<"bucket">>, Props),
    Key = proplists:get_value(<<"key">>, Props),
    Clock = proplists:get_value(<<"clock">>, Props),
    BucketAndType =
        case BType of
            <<"default">> ->
                Bucket;
            Type when is_binary(Type) ->
                {Type, Bucket}
        end,
    {{BucketAndType, Key}, base64:decode(Clock)}.


-spec erlify_aae_buckets(list()) -> list(riakc_obj:bucket()).
erlify_aae_buckets(BucketList) ->
    lists:map(fun erlify_aae_bucket/1, BucketList).

-spec erlify_aae_bucket({struct, proplists:proplist()}) -> riakc_obj:bucket().
erlify_aae_bucket({struct, BucketProps}) ->
    BType = proplists:get_value(<<"bucket-type">>, BucketProps, <<"default">>),
    Bucket = proplists:get_value(<<"bucket">>, BucketProps),
    case BType of
        <<"default">> ->
            Bucket;
        Type when is_binary(Type) ->
            {Type, Bucket}
    end.

%% @doc Convert a stats-resource response to an erlang-term server
%%      information proplist.
erlify_server_info(Props) ->
    lists:flatten([ erlify_server_info(K, V) || {K, V} <- Props ]).
erlify_server_info(<<"nodename">>, Name) -> {node, Name};
erlify_server_info(<<"riak_kv_version">>, Vsn) -> {server_version, Vsn};
erlify_server_info(_Ignore, _) -> [].

%% @doc Convert a preflist resource response to a proplist.
erlify_preflist(Response) ->
    Preflist = [V || {_, V} <- proplists:get_value(<<"preflist">>, Response)],
    {<<"partition">>, Partition} = lists:keyfind(<<"partition">>, 1, Preflist),
    {<<"node">>, Node} = lists:keyfind(<<"node">>, 1, Preflist),
    {<<"primary">>, IfPrimary} = lists:keyfind(<<"primary">>, 1, Preflist),
    #preflist_item{partition = Partition, node = Node, primary = IfPrimary}.


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
            SSLOpts = 
                case proplists:get_value(ssl_options, Options, []) of
                    X when is_list(X) ->
                        lists:ukeysort(1, X ++ ?DEFAULT_TLS_OPTIONS);
                    _ ->
                        ?DEFAULT_TLS_OPTIONS
                end,
            [{is_ssl, true}, {ssl_options, SSLOpts}];
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

-ifdef(TEST).
%% validate that bucket, keys and link specifications do not contain
%% unescaped slashes
%%
%% See section on URL Escaping information at
%% http://docs.basho.com/riak/latest/dev/references/http/

url_escaping_test() ->
	Rhc = create(),
	Type = "my/type",
	Bucket = "my/bucket",
	Key = "my/key",
	Query = [],

	Url = iolist_to_binary(make_url(Rhc, {Type, Bucket}, Key, [])),
	ExpectedUrl =
            <<"http://127.0.0.1:8098/types/my%2Ftype/buckets/my%2Fbucket/keys/my%2Fkey/">>,
	?assertEqual(ExpectedUrl, Url),

	CounterUrl = iolist_to_binary(make_counter_url(Rhc, Bucket, Key, Query)),
	ExpectedCounterUrl = <<"http://127.0.0.1:8098/buckets/my%2Fbucket/counters/my%2Fkey?">>,
	?assertEqual(ExpectedCounterUrl, CounterUrl),

	IndexUrl = iolist_to_binary(
                     index_url(Rhc, {Type, Bucket}, {binary_index, "mybinaryindex"}, Query, [])),
	ExpectedIndexUrl =
            <<"http://127.0.0.1:8098/types/my%2Ftype/buckets/my%2Fbucket/index/mybinaryindex_bin">>,
	?assertEqual(ExpectedIndexUrl, IndexUrl),

	ok.

-endif.

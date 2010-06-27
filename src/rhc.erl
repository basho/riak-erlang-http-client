-module(rhc).

-export([create/0, create/4,
         ip/1,
         port/1,
         prefix/1,
         options/1,
         ping/1,
         get_client_id/1,
         get_server_info/1,
         get/3, get/4,
         put/2, put/3,
         delete/3, delete/4,
         list_buckets/1,
         list_keys/2,
         stream_list_keys/2,
         get_bucket/2,
         set_bucket/3,
         mapred/3,mapred/4,
         mapred_stream/4, mapred_stream/5,
         mapred_bucket/3, mapred_bucket/4,
         mapred_bucket_stream/5]).

-include("raw_http.hrl").
-include("rhc.hrl").

create() ->
    create("127.0.0.1", 8098, "riak", []).

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

ip(#rhc{ip=IP}) -> IP.

port(#rhc{port=Port}) -> Port.

prefix(#rhc{prefix=Prefix}) -> Prefix.

ping(Rhc) ->
    Url = ping_url(Rhc),
    case request(get, Url, ["200","204"]) of
        {ok, _Status, _Headers, _Body} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

get_client_id(Rhc) ->
    {ok, client_id(Rhc, [])}.

get_server_info(Rhc) ->
    Url = stats_url(Rhc),
    case request(get, Url, ["200"]) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_server_info(Response)};
        {error, Error} ->
            {error, Error}
    end.

get(Rhc, Bucket, Key) ->
    get(Rhc, Bucket, Key, []).

get(Rhc, Bucket, Key, Options) ->
    Qs = get_q_params(Rhc, Options),
    Url = make_url(Rhc, Bucket, Key, Qs),
    case request(get, Url, ["200", "300"]) of
        {ok, _Status, Headers, Body} ->
            {ok, rhc_obj:make_riakc_obj(Bucket, Key, Headers, Body)};
        {error, {ok, "404", _, _}} ->
            {error, notfound};
        {error, Error} ->
            {error, Error}
    end.

put(Rhc, Object) ->
    put(Rhc, Object, []).

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
    case request(Method, Url, ["200", "204", "300"], Headers, Body) of
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
    
delete(Rhc, Bucket, Key) ->
    delete(Rhc, Bucket, Key, []).

delete(Rhc, Bucket, Key, Options) ->
    Qs = delete_q_params(Rhc, Options),
    Url = make_url(Rhc, Bucket, Key, Qs),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
    case request(delete, Url, ["204"], Headers) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.
    
list_buckets(_Rhc) ->
    throw(not_implemented).

list_keys(Rhc, Bucket) ->
    {ok, ReqId} = stream_list_keys(Rhc, Bucket),
    rhc_listkeys:wait_for_listkeys(ReqId, ?DEFAULT_TIMEOUT).

%% @doc Stream key lists to a Pid.  Messages sent to the Pid will
%%      be of the form {reference(), message()} where message()
%%      is one of:
%%         done -- end of key list, no more messages will be sent
%%         {keys, [key()]}] -- a portion of the key list
%%         {error, term()} -- an error occurred
%% @spec stream_list_keys(rhc(), bucket()) ->
%%          {ok, reference()}|{error, term()}
stream_list_keys(Rhc, Bucket) ->
    Url = make_url(Rhc, Bucket, undefined, [{?Q_KEYS, ?Q_STREAM},
                                            {?Q_PROPS, ?Q_FALSE}]),
    StartRef = make_ref(),
    Pid = spawn(rhc_listkeys, list_keys_acceptor, [self(), StartRef]),
    case request_stream(Pid, get, Url) of
        {ok, ReqId}    ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} -> {error, Error}
    end.

get_bucket(Rhc, Bucket) ->
    Url = make_url(Rhc, Bucket, undefined, [{?Q_KEYS, ?Q_FALSE}]),
    case request(get, Url, ["200"]) of
        {ok, "200", _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {struct, Props} = proplists:get_value(?JSON_PROPS, Response),
            {ok, rhc_bucket:erlify_props(Props)};
        {error, Error} ->
            {error, Error}
    end.

set_bucket(Rhc, Bucket, Props0) ->
    Url = make_url(Rhc, Bucket, undefined, []),
    Headers =  [{"Content-Type", "application/json"}],
    Props = rhc_bucket:httpify_props(Props0),
    Body = mochijson2:encode({struct, [{?Q_PROPS, {struct, Props}}]}),
    case request(put, Url, ["204"], Headers, Body) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.

mapred(Rhc, Inputs, Query) ->
    mapred(Rhc, Inputs, Query, ?DEFAULT_TIMEOUT).

mapred(Rhc, Inputs, Query, Timeout) ->
    {ok, ReqId} = mapred_stream(Rhc, Inputs, Query, self(), Timeout),
    rhc_mapred:wait_for_mapred(ReqId, Timeout).

mapred_stream(Rhc, Inputs, Query, ClientPid) ->
    mapred_stream(Rhc, Inputs, Query, ClientPid, ?DEFAULT_TIMEOUT).

%% @doc Stream map/reduce results to a Pid.  Messages sent to the Pid
%%      will be of the form {reference(), message()}, where message()
%%      is one of:
%%         done} -- query has completed, no more messages will be sent
%%         {mapred, integer(), mochijson()} -- partial results of a query
%%              the second item in the tuple is the (zero-indexed) phase
%%              number, and the third is the JSON-decoded results
%%         {error, term()} - an error occurred
%% @spec mapred_stream(rhc(), mapred_input(), [query_phase()],
%%                     pid(), integer())
%%          -> {ok, reference()}|{error, term()}
mapred_stream(Rhc, Inputs, Query, ClientPid, Timeout) ->
    Url = mapred_url(Rhc),
    StartRef = make_ref(),
    Pid = spawn(rhc_mapred, mapred_acceptor, [ClientPid, StartRef]),
    Headers = [{?HEAD_CTYPE, "application/json"}],
    Body = rhc_mapred:encode_mapred(Inputs, Query),
    case request_stream(Pid, post, Url, Headers, Body) of
        {ok, ReqId} ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} -> {error, Error}
    end.

mapred_bucket(Rhc, Bucket, Query) ->
    mapred_bucket(Rhc, Bucket, Query, ?DEFAULT_TIMEOUT).

mapred_bucket(Rhc, Bucket, Query, Timeout) ->
    {ok, ReqId} = mapred_bucket_stream(Rhc, Bucket, Query, self(), Timeout),
    rhc_mapred:wait_for_mapred(ReqId, Timeout).

mapred_bucket_stream(Rhc, Bucket, Query, ClientPid, Timeout) ->
    mapred_stream(Rhc, Bucket, Query, ClientPid, Timeout).

%% INTERNAL

client_id(#rhc{options=RhcOptions}, Options) ->
    case proplists:get_value(client_id, Options) of
        undefined ->
            proplists:get_value(client_id, RhcOptions);
        ClientId ->
            ClientId
    end.

random_client_id() ->
    {{Y,Mo,D},{H,Mi,S}} = erlang:universaltime(),
    {_,_,NowPart} = now(),
    Id = erlang:phash2([Y,Mo,D,H,Mi,S,node(),NowPart]),
    base64:encode_to_string(<<Id:32>>).

root_url(#rhc{ip=Ip, port=Port}) ->
    ["http://",Ip,":",integer_to_list(Port),"/"].

mapred_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "mapred/?chunked=true"])).

ping_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "ping/"])).

stats_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "stats/"])).
    
make_url(Rhc=#rhc{prefix=Prefix}, Bucket, Key, Query) ->
    binary_to_list(
      iolist_to_binary(
        [root_url(Rhc),
         Prefix, "/",
         Bucket, "/",
         [ [Key,"/"] || Key =/= undefined ],
         [ ["?", mochiweb_util:urlencode(Query)] || Query =/= [] ]
        ])).

request(Method, Url, Expect) ->
    request(Method, Url, Expect, [], []).
request(Method, Url, Expect, Headers) ->
    request(Method, Url, Expect, Headers, []).
request(Method, Url, Expect, Headers, Body) ->
    Accept = {"Accept", "multipart/mixed, */*;q=0.9"},
    case ibrowse:send_req(Url, [Accept|Headers], Method, Body) of
        Resp={ok, Status, _, _} ->
            case lists:member(Status, Expect) of
                true -> Resp;
                false -> {error, Resp}
            end;
        Error ->
            Error
    end.

request_stream(Pid, Method, Url) ->
    request_stream(Pid, Method, Url, []).
request_stream(Pid, Method, Url, Headers) ->
    request_stream(Pid, Method, Url, Headers, []).
request_stream(Pid, Method, Url, Headers, Body) ->
    case ibrowse:send_req(Url, Headers, Method, Body, [{stream_to, {Pid,once}}]) of
        {ibrowse_req_id, ReqId} ->
            {ok, ReqId};
        Error ->
            Error
    end.

options(#rhc{options=Options}) ->
    Options.

get_q_params(Rhc, Options) ->
    options_list([r], Options ++ options(Rhc)).

put_q_params(Rhc, Options) ->
    options_list([r,w,dw,{return_body,"returnbody"}],
                 Options ++ options(Rhc)).

delete_q_params(Rhc, Options) ->
    options_list([r,rw], Options ++ options(Rhc)).

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

erlify_server_info(Props) ->
    lists:flatten([ erlify_server_info(K, V) || {K, V} <- Props ]).
erlify_server_info(<<"nodename">>, Name) -> {node, Name};
erlify_server_info(<<"riak_kv_version">>, Vsn) -> {server_version, Vsn};
erlify_server_info(_Ignore, _) -> [].


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
-export([list_keys_acceptor/2,
         mapred_acceptor/2]).

-include("raw_http.hrl").

-define(DEFAULT_TIMEOUT, 60000).

-record(rhc, {ip,
              port,
              prefix,
              options}).

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
            {ok, make_riakc_obj(Bucket, Key, Headers, Body)};
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
    {Headers0, Body} = serialize_riakc_obj(Rhc, Object),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}
               |Headers0],
    case request(Method, Url, ["200", "204", "300"], Headers, Body) of
        {ok, Status, ReplyHeaders, ReplyBody} ->
            if Status =:= "204" ->
                    ok;
               true ->
                    {ok, make_riakc_obj(Bucket, Key, ReplyHeaders, ReplyBody)}
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
    wait_for_listkeys(ReqId, ?DEFAULT_TIMEOUT).

stream_list_keys(Rhc, Bucket) ->
    Url = make_url(Rhc, Bucket, undefined, [{?Q_KEYS, ?Q_STREAM},
                                            {?Q_PROPS, ?Q_FALSE}]),
    StartRef = make_ref(),
    Pid = spawn(rhc, list_keys_acceptor, [self(), StartRef]),
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
            {ok, erlify_bucket_props(Props)};
        {error, Error} ->
            {error, Error}
    end.

set_bucket(Rhc, Bucket, Props0) ->
    Url = make_url(Rhc, Bucket, undefined, []),
    Headers =  [{"Content-Type", "application/json"}],
    Props = httpify_bucket_props(Props0),
    Body = mochijson2:encode({struct, [{?Q_PROPS, {struct, Props}}]}),
    case request(put, Url, ["204"], Headers, Body) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.

mapred(Rhc, Inputs, Query) ->
    mapred(Rhc, Inputs, Query, ?DEFAULT_TIMEOUT).

mapred(Rhc, Inputs, Query, Timeout) ->
    {ok, ReqId} = mapred_stream(Rhc, Inputs, Query, self(), Timeout),
    wait_for_mapred(ReqId, Timeout).

mapred_stream(Rhc, Inputs, Query, ClientPid) ->
    mapred_stream(Rhc, Inputs, Query, ClientPid, ?DEFAULT_TIMEOUT).

mapred_stream(Rhc, Inputs, Query, ClientPid, Timeout) ->
    Url = mapred_url(Rhc),
    StartRef = make_ref(),
    Pid = spawn(rhc, mapred_acceptor, [ClientPid, StartRef]),
    Headers = [{?HEAD_CTYPE, "application/json"}],
    Body = encode_mapred(Inputs, Query),
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
    wait_for_mapred(ReqId, Timeout).

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

make_riakc_obj(Bucket, Key, Headers, Body) ->
    Vclock = base64:decode(proplists:get_value(?HEAD_VCLOCK, Headers, "")),
    case ctype_from_headers(Headers) of
        {"multipart/mixed", Args} ->
            {"boundary", Boundary} = proplists:lookup("boundary", Args),
            riakc_obj:new_obj(
              Bucket, Key, Vclock,
              decode_siblings(Boundary, Body));
        {_CType, _} ->
            riakc_obj:new_obj(
              Bucket, Key, Vclock,
              [{headers_to_metadata(Headers), list_to_binary(Body)}])
    end.

ctype_from_headers(Headers) ->
    mochiweb_util:parse_header(
      proplists:get_value(?HEAD_CTYPE, Headers)).

vtag_from_headers(Headers) ->
    %% non-sibling uses ETag, sibling uses Etag
    %% (note different capitalization on 't')
    case proplists:lookup("ETag", Headers) of
        {"ETag", ETag} -> ETag;
        none -> proplists:get_value("Etag", Headers)
    end.
           

lastmod_from_headers(Headers) ->
    case proplists:get_value("Last-Modified", Headers) of
        undefined ->
            undefined;
        RfcDate ->
            GS = calendar:datetime_to_gregorian_seconds(
                   httpd_util:convert_request_date(RfcDate)),
            ES = GS-62167219200, %% gregorian seconds of the epoch
            {ES div 1000000, % Megaseconds
             ES rem 1000000, % Seconds
             0}              % Microseconds
    end.

decode_siblings(Boundary, "\r\n"++SibBody) ->
    decode_siblings(Boundary, SibBody);
decode_siblings(Boundary, SibBody) ->
    Parts = webmachine_multipart:get_all_parts(
              list_to_binary(SibBody), Boundary),
    [ {headers_to_metadata([ {binary_to_list(H), binary_to_list(V)}
                             || {H, V} <- Headers ]),
       element(1, split_binary(Body, size(Body)-2))} %% remove trailing \r\n
      || {_, {_, Headers}, Body} <- Parts ].

headers_to_metadata(Headers) ->
    UserMeta = extract_user_metadata(Headers),

    {CType,_} = ctype_from_headers(Headers),
    CUserMeta = dict:store(?MD_CTYPE, CType, UserMeta),

    VTag = vtag_from_headers(Headers),
    VCUserMeta = dict:store(?MD_VTAG, VTag, CUserMeta),

    LVCUserMeta = case lastmod_from_headers(Headers) of
                      undefined ->
                          VCUserMeta;
                      LastMod ->
                          dict:store(?MD_LASTMOD, LastMod, VCUserMeta)
                  end,

    case extract_links(Headers) of
        [] -> LVCUserMeta;
        Links -> dict:store(?MD_LINKS, Links, LVCUserMeta)
    end.

extract_user_metadata(_Headers) ->
    %%TODO
    dict:new().

extract_links(Headers) ->
    {ok, Re} = re:compile("</[^/]+/([^/]+)/([^/]+)>; *riaktag=\"(.*)\""),
    Extractor = fun(L, Acc) ->
                        case re:run(L, Re, [{capture,[1,2,3],binary}]) of
                            {match, [Bucket, Key,Tag]} ->
                                [{{Bucket,Key},Tag}|Acc];
                            nomatch ->
                                Acc
                        end
                end,
    LinkHeader = proplists:get_value(?HEAD_LINK, Headers, []),
    lists:foldl(Extractor, [], string:tokens(LinkHeader, ",")).

serialize_riakc_obj(Rhc, Object) ->
    {make_headers(Rhc, Object), make_body(Object)}.

make_headers(Rhc, Object) ->
    MD = riakc_obj:get_update_metadata(Object),
    CType = case dict:find(?MD_CTYPE, MD) of
                {ok, C} when is_list(C) -> C;
                {ok, C} when is_binary(C) -> binary_to_list(C);
                error -> "application/octet-stream"
            end,
    Links = case dict:find(?MD_LINKS, MD) of
                {ok, L} -> L;
                error   -> []
            end,
    VClock = riakc_obj:vclock(Object),
    lists:flatten(
      [{?HEAD_CTYPE, CType},
       [ {?HEAD_LINK, encode_links(Rhc, Links)} || Links =/= [] ],
       [ {?HEAD_VCLOCK, base64:encode_to_string(VClock)}
         || VClock =/= undefined ]
       | encode_user_metadata(MD) ]).

encode_links(_, []) -> [];
encode_links(#rhc{prefix=Prefix}, Links) ->
    {{FirstBucket, FirstKey}, FirstTag} = hd(Links),
    lists:foldl(
      fun({{Bucket, Key}, Tag}, Acc) ->
              [format_link(Prefix, Bucket, Key, Tag), ", "|Acc]
      end,
      format_link(Prefix, FirstBucket, FirstKey, FirstTag),
      tl(Links)).

encode_user_metadata(_Metadata) ->
    %% TODO
    [].

format_link(Prefix, Bucket, Key, Tag) ->
    io_lib:format("</~s/~s/~s>; riaktag=\"~s\"",
                  [Prefix, Bucket, Key, Tag]).

make_body(Object) ->
    case riakc_obj:get_update_value(Object) of
        Val when is_binary(Val) -> Val;
        Val when is_list(Val) ->
            case is_iolist(Val) of
                true -> Val;
                false -> term_to_binary(Val)
            end;
        Val ->
            term_to_binary(Val)
    end.

is_iolist(Binary) when is_binary(Binary) -> true;
is_iolist(List) when is_list(List) ->
    lists:all(fun is_iolist/1, List);
is_iolist(_) -> false.

erlify_bucket_props(Props) ->
    lists:flatten([ erlify_bucket_prop(K, V) || {K, V} <- Props ]).
erlify_bucket_prop(?JSON_N_VAL, N) -> {n_val, N};
erlify_bucket_prop(?JSON_ALLOW_MULT, AM) -> {allow_mult, AM};
erlify_bucket_prop(_Ignore, _) -> [].

httpify_bucket_props(Props) ->
    lists:flatten([ httpify_bucket_prop(K, V) || {K, V} <- Props ]).
httpify_bucket_prop(n_val, N) -> {?JSON_N_VAL, N};
httpify_bucket_prop(allow_mult, AM) -> {?JSON_ALLOW_MULT, AM};
httpify_bucket_prop(_Ignore, _) -> [].

erlify_server_info(Props) ->
    lists:flatten([ erlify_server_info(K, V) || {K, V} <- Props ]).
erlify_server_info(<<"nodename">>, Name) -> {node, Name};
erlify_server_info(<<"riak_kv_version">>, Vsn) -> {server_version, Vsn};
erlify_server_info(_Ignore, _) -> [].


list_keys_acceptor(Pid, PidRef) ->
    receive
        {ibrowse_req_id, PidRef, IbrowseRef} ->
            list_keys_acceptor(Pid,PidRef,IbrowseRef,[])
    after ?DEFAULT_TIMEOUT ->
            Pid ! {PidRef, {error, {timeout, []}}}
    end.

list_keys_acceptor(Pid,PidRef,IbrowseRef,Buffer) ->
    receive
        {ibrowse_async_response_end, IbrowseRef} ->
            if Buffer =:= [] ->
                    Pid ! {PidRef, done};
               true ->
                    Pid ! {PidRef, {error, {not_parseable, Buffer}}}
            end;
        {ibrowse_async_response, IbrowseRef, {error,Error}} ->
            Pid ! {PidRef, {error, Error}};
        {ibrowse_async_response, IbrowseRef, []} ->
            %% ignore empty data
            ibrowse:stream_next(IbrowseRef),
            list_keys_acceptor(Pid,PidRef,IbrowseRef,Buffer);
        {ibrowse_async_response, IbrowseRef, Data} ->
            case catch mochijson2:decode([Buffer,Data]) of
                {struct, Response} ->
                    Keys = proplists:get_value(?JSON_KEYS, Response, []),
                    Pid ! {PidRef, {keys, Keys}},
                    ibrowse:stream_next(IbrowseRef),
                    list_keys_acceptor(Pid,PidRef,IbrowseRef,[]);
                {'EXIT', _} ->
                    ibrowse:stream_next(IbrowseRef),
                    list_keys_acceptor(Pid,PidRef,IbrowseRef,
                                       [Buffer,Data])
            end;
        {ibrowse_async_headers, IbrowseRef, Status, Headers} ->
            if Status =/= "200" ->
                    Pid ! {PidRef, {error, {Status, Headers}}};
               true ->
                    ibrowse:stream_next(IbrowseRef),
                    list_keys_acceptor(Pid,PidRef,IbrowseRef,Buffer)
            end
    after ?DEFAULT_TIMEOUT ->
            Pid ! {PidRef, {error, timeout}}
    end.

%% @private
wait_for_listkeys(ReqId, Timeout) ->
    wait_for_listkeys(ReqId,Timeout,[]).
%% @private
wait_for_listkeys(ReqId,Timeout,Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId, {keys,Res}} -> wait_for_listkeys(ReqId,Timeout,[Res|Acc]);
        {ReqId, {error, Reason}} -> {error, Reason}
    after Timeout ->
            {error, {timeout, Acc}}
    end.

mapred_acceptor(Pid, PidRef) ->
    receive
        {ibrowse_req_id, PidRef, IbrowseRef} ->
            mapred_acceptor(Pid,PidRef,IbrowseRef)
    after ?DEFAULT_TIMEOUT ->
            Pid ! {PidRef, {error, {timeout, []}}}
    end.

mapred_acceptor(Pid,PidRef,IbrowseRef) ->
    receive
        {ibrowse_async_headers, IbrowseRef, Status, Headers} ->
            if Status =/= "200" ->
                    Pid ! {PidRef, {error, {Status, Headers}}};
               true ->
                    {"multipart/mixed", Args} = ctype_from_headers(Headers),
                    {"boundary", Boundary} =
                        proplists:lookup("boundary", Args),
                    stream_parts_acceptor(
                      Pid, PidRef,
                      webmachine_multipart:stream_parts(
                        {[],stream_parts_helper(Pid,PidRef,IbrowseRef,true)},
                        Boundary))
            end
    after ?DEFAULT_TIMEOUT ->
            Pid ! {PidRef, {error, timeout}}
    end.

stream_parts_acceptor(Pid,PidRef,done_parts) ->
    Pid ! {PidRef, done};
stream_parts_acceptor(Pid,PidRef,{{_Name, _Param, Part},Next}) ->
    {struct, Response} = mochijson2:decode(Part),
    Phase = proplists:get_value(<<"phase">>, Response),
    Res = proplists:get_value(<<"data">>, Response),
    Pid ! {PidRef, {mapred, Phase, Res}},
    stream_parts_acceptor(Pid,PidRef,Next()).


stream_parts_helper(Pid, PidRef, IbrowseRef, First) ->              
    fun() ->
            ibrowse:stream_next(IbrowseRef),
            receive
                {ibrowse_async_response_end, IbrowseRef} ->
                    {<<>>,done};
                {ibrowse_async_response, IbrowseRef, {error, Error}} ->
                    Pid ! {PidRef, {error, Error}},
                    throw({error, {ibrowse, Error}});
                {ibrowse_async_response, IbrowseRef, []} ->
                    Fun = stream_parts_helper(Pid, PidRef, IbrowseRef, First),
                    Fun();
                {ibrowse_async_response, IbrowseRef, Data0} ->
                    Data = if First ->
                                   case Data0 of
                                       "\n"++D -> D;
                                       "\r\n"++D -> D;
                                       _ -> Data0
                                   end;
                              true ->
                                   Data0
                           end,
                    {list_to_binary(Data),
                     stream_parts_helper(Pid, PidRef, IbrowseRef,false)}
            after ?DEFAULT_TIMEOUT ->
                    Pid ! {PidRef, {error, timeout}},
                    throw({error, {ibrowse, timeout}})
            end
    end.

%% @private
wait_for_mapred(ReqId, Timeout) ->
    wait_for_mapred(ReqId,Timeout,orddict:new()).
%% @private
wait_for_mapred(ReqId, Timeout, Acc) ->
    receive
        {ReqId, done} -> {ok, orddict:to_list(Acc)};
        {ReqId, {mapred,Phase,Res}} ->
            wait_for_mapred(ReqId,Timeout,orddict:append_list(Phase,Res,Acc));
        {ReqId, {error, Reason}} -> {error, Reason}
    after Timeout ->
            {error, {timeout, orddict:to_list(Acc)}}
    end.


encode_mapred(Inputs, Query) ->
    mochijson2:encode(
      {struct, [{<<"inputs">>, encode_mapred_inputs(Inputs)},
                {<<"query">>, encode_mapred_query(Query)}]}).

encode_mapred_inputs(Bucket) when is_binary(Bucket) ->
    Bucket;
encode_mapred_inputs(Keylist) when is_list(Keylist) ->
    [ normalize_mapred_input(I) || I <- Keylist ].

normalize_mapred_input({Bucket, Key})
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key];
normalize_mapred_input({{Bucket, Key}, KeyData})
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key, KeyData];
normalize_mapred_input([Bucket, Key])
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key];
normalize_mapred_input([Bucket, Key, KeyData])
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key, KeyData].

encode_mapred_query(Query) when is_list(Query) ->
    [ encode_mapred_phase(P) || P <- Query ].

encode_mapred_phase({MR, Fundef, Arg, Keep}) when MR =:= map;
                                                  MR =:= reduce ->
    Type = if MR =:= map -> <<"map">>;
              MR =:= reduce -> <<"reduce">>
           end,
    {Lang, Json} = case Fundef of
                       {modfun, Mod, Fun} ->
                           {<<"erlang">>,
                            [{<<"module">>,
                              list_to_binary(atom_to_list(Mod))},
                             {<<"function">>,
                              list_to_binary(atom_to_list(Fun))}]};
                       {jsfun, Name} ->
                           {<<"javascript">>,
                            [{<<"name">>, Name}]};
                       {jsanon, {Bucket, Key}} ->
                           {<<"javascript">>,
                            [{<<"bucket">>, Bucket},
                             {<<"key">>, Key}]};
                       {jsanon, Source} ->
                           {<<"javascript">>,
                            [{<<"source">>, Source}]}
                   end,
    {struct,
     [{Type,
       {struct, [{<<"language">>, Lang},
                 {<<"arg">>, Arg},
                 {<<"keep">>, Keep}
                 |Json
                ]}
      }]};
encode_mapred_phase({link, Bucket, Tag, Keep}) ->
    {struct,
     [{<<"link">>,
       {struct, [{<<"bucket">>, if Bucket =:= '_' -> <<"_">>;
                                   true           -> Bucket
                                end},
                 {<<"tag">>, if Tag =:= '_' -> <<"_">>;
                                true        -> Tag
                             end},
                 {<<"keep">>, Keep}
                 ]}
       }]}.


                           

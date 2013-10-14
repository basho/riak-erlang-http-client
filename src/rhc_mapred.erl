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
%%      encode and decode map/reduce queries.
-module(rhc_mapred).

-export([encode_mapred/2,
         wait_for_mapred/2]).
%% spawnable exports
-export([mapred_acceptor/3]).


%%% REQUEST ENCODING

%% @doc Translate erlang-term map/reduce query into JSON format.
%% @spec encode_mapred(map_input(), [query_part()]) -> iolist()
%% @type map_input() = bucket()|[key_spec()]|
%%                     {modfun, atom(), atom(), term()}
%% @type key_spec() = {bucket(), key()}|{{bucket(), key()},tag()}
%% @type bucket() = binary()
%% @type key() = binary()
%% @type tag() = binary()
%% @type query_part() = {map, funspec(), binary(), boolean()}|
%%                      {reduce, funspec(), binary(), boolean()}|
%%                      {link, linkspec(), linkspec(), boolean()}
%% @type funspec() = {modfun, atom(), atom()}|
%%                   {jsfun, binary()}|
%%                   {jsanon, {bucket(), key()}}|
%%                   {jsanon, binary()}
%% @type linkspec() = binary()|'_'
encode_mapred(Inputs, Query) ->
    mochijson2:encode(
      {struct, [{<<"inputs">>, encode_mapred_inputs(Inputs)},
                {<<"query">>, encode_mapred_query(Query)}]}).
encode_mapred_inputs({BucketType, Bucket}) when is_binary(BucketType),
                                                is_binary(Bucket) ->
    [BucketType, Bucket];
encode_mapred_inputs(Bucket) when is_binary(Bucket) ->
    Bucket;
encode_mapred_inputs(Keylist) when is_list(Keylist) ->
    [ normalize_mapred_input(I) || I <- Keylist ];
encode_mapred_inputs({index, Bucket, Index, Key}) ->
    {struct, [{<<"bucket">>, encode_mapred_inputs(Bucket)},
              {<<"index">>, riakc_obj:index_id_to_bin(Index)},
              {<<"key">>, Key}]};
encode_mapred_inputs({index, Bucket, Index, StartKey, EndKey}) ->
    {struct, [{<<"bucket">>, encode_mapred_inputs(Bucket)},
              {<<"index">>, riakc_obj:index_id_to_bin(Index)},
              {<<"start">>, StartKey},
              {<<"end">>, EndKey}]};
encode_mapred_inputs({modfun, Module, Function, Options}) ->
    {struct, [{<<"module">>, atom_to_binary(Module, utf8)},
              {<<"function">>, atom_to_binary(Function, utf8)},
              {<<"arg">>, Options}]}.

%% @doc Normalize all bucket-key-data inputs to either
%%        [Bucket, Key]
%%      or
%%        [Bucket, Key, KeyData]
normalize_mapred_input({Bucket, Key})
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key];
normalize_mapred_input({{{Type, Bucket}, Key}, KeyData})
    when is_binary(Type), is_binary(Bucket), is_binary(Key) ->
    [Type, Bucket, Key, KeyData];
normalize_mapred_input({{Bucket, Key}, KeyData})
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key, KeyData];
normalize_mapred_input([Type, Bucket, Key, _KeyData]=List)
    when is_binary(Type), is_binary(Bucket), is_binary(Key) ->
    List;
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
                              atom_to_binary(Mod, utf8)},
                             {<<"function">>,
                              atom_to_binary(Fun, utf8)}]};
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

%%% RESPONSE DECODING


%% @doc Collect all mapreduce results, and provide them as one value
%%      instead of streaming to a Pid.
%% @spec wait_for_mapred(term(), integer()) ->
%%            {ok, [phase_result()]}|{error, term()}
%% @type phase_result() = {integer(), [term()]}
wait_for_mapred(ReqId, Timeout) ->
    wait_for_mapred_first(ReqId, Timeout).

%% Wait for the first mapred result, so we know at least one phase
%% that will be delivering results.
wait_for_mapred_first(ReqId, Timeout) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, []};
        {mapred, Phase, Res} ->
            wait_for_mapred_one(ReqId, Timeout, Phase,
                                acc_mapred_one(Res, []));
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, []}}
    end.

%% So far we have only received results from one phase.  This method
%% of accumulating a single phases's outputs will be more efficient
%% than the repeated orddict:append_list/3 used when accumulating
%% outputs from multiple phases.
wait_for_mapred_one(ReqId, Timeout, Phase, Acc) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, finish_mapred_one(Phase, Acc)};
        {mapred, Phase, Res} ->
            %% still receiving for just one phase
            wait_for_mapred_one(ReqId, Timeout, Phase,
                                acc_mapred_one(Res, Acc));
        {mapred, NewPhase, Res} ->
            %% results from a new phase have arrived - track them all
            Dict = [{NewPhase, Res},{Phase, Acc}],
            wait_for_mapred_many(ReqId, Timeout, Dict);
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, finish_mapred_one(Phase, Acc)}}
    end.

%% Single-phase outputs are kept as a reverse list of results.
acc_mapred_one([R|Rest], Acc) ->
    acc_mapred_one(Rest, [R|Acc]);
acc_mapred_one([], Acc) ->
    Acc.

finish_mapred_one(Phase, Acc) ->
    [{Phase, lists:reverse(Acc)}].

%% Tracking outputs from multiple phases.
wait_for_mapred_many(ReqId, Timeout, Acc) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, finish_mapred_many(Acc)};
        {mapred, Phase, Res} ->
            wait_for_mapred_many(
              ReqId, Timeout, acc_mapred_many(Phase, Res, Acc));
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, finish_mapred_many(Acc)}}
    end.

%% Many-phase outputs are kepts as a proplist of reversed lists of
%% results.
acc_mapred_many(Phase, Res, Acc) ->
    case lists:keytake(Phase, 1, Acc) of
        {value, {Phase, PAcc}, RAcc} ->
            [{Phase,acc_mapred_one(Res,PAcc)}|RAcc];
        false ->
            [{Phase,acc_mapred_one(Res,[])}|Acc]
    end.

finish_mapred_many(Acc) ->
    [ {P, lists:reverse(A)} || {P, A} <- lists:keysort(1, Acc) ].

%% Receive one mapred message.
-spec receive_mapred(reference(), timeout()) ->
         done | {mapred, integer(), [term()]} | {error, term()} | timeout.
receive_mapred(ReqId, Timeout) ->
    receive {ReqId, Msg} ->
            %% Msg should be `done', `{mapred, Phase, Results}', or
            %% `{error, Reason}'
            Msg
    after Timeout ->
            timeout
    end.

%% @doc first stage of ibrowse response handling - just waits to be
%%      told what ibrowse request ID to expect
mapred_acceptor(Pid, PidRef, Timeout) ->
    receive
        {ibrowse_req_id, PidRef, IbrowseRef} ->
            mapred_acceptor(Pid,PidRef,Timeout,IbrowseRef)
    after Timeout ->
            Pid ! {PidRef, {error, {timeout, []}}}
    end.

%% @doc second stage of ibrowse response handling - waits for headers
%%      and extracts the boundary of the multipart/mixed message
mapred_acceptor(Pid,PidRef,Timeout,IbrowseRef) ->
    receive
        {ibrowse_async_headers, IbrowseRef, Status, Headers} ->
            if Status =/= "200" ->
                    Pid ! {PidRef, {error, {Status, Headers}}};
               true ->
                    {"multipart/mixed", Args} =
                        rhc_obj:ctype_from_headers(Headers),
                    {"boundary", Boundary} =
                        proplists:lookup("boundary", Args),
                    stream_parts_acceptor(
                      Pid, PidRef,
                      webmachine_multipart:stream_parts(
                        {[],stream_parts_helper(Pid,PidRef,Timeout,
                                                IbrowseRef,true)},
                        Boundary))
            end
    after Timeout ->
            Pid ! {PidRef, {error, timeout}}
    end.

%% @doc driver of the webmachine_multipart streamer - handles results
%%      of the parsing process (sends them to the client) and polls for
%%      the next part
stream_parts_acceptor(Pid,PidRef,done_parts) ->
    Pid ! {PidRef, done};
stream_parts_acceptor(Pid,PidRef,{{_Name, _Param, Part},Next}) ->
    {struct, Response} = mochijson2:decode(Part),
    Phase = proplists:get_value(<<"phase">>, Response),
    Res = proplists:get_value(<<"data">>, Response),
    Pid ! {PidRef, {mapred, Phase, Res}},
    stream_parts_acceptor(Pid,PidRef,Next()).

%% @doc "next" fun for the webmachine_multipart streamer - waits for
%%      an ibrowse message, and then returns it to the streamer for processing
stream_parts_helper(Pid, PidRef, Timeout, IbrowseRef, First) ->              
    fun() ->
            receive
                {ibrowse_async_response_end, IbrowseRef} ->
                    {<<>>,done};
                {ibrowse_async_response, IbrowseRef, {error, Error}} ->
                    Pid ! {PidRef, {error, Error}},
                    throw({error, {ibrowse, Error}});
                {ibrowse_async_response, IbrowseRef, []} ->
                    Fun = stream_parts_helper(Pid, PidRef, Timeout,
                                              IbrowseRef, First),
                    Fun();
                {ibrowse_async_response, IbrowseRef, Data0} ->
                    %% the streamer doesn't like the body to start with
                    %% CRLF, so strip that off on the first chunk
                    Data = if First ->
                                   case Data0 of
                                       <<"\n",D/binary>> -> D;
                                       <<"\r\n",D/binary>> -> D;
                                       _ -> Data0
                                   end;
                              true ->
                                   Data0
                           end,
                    {Data,
                     stream_parts_helper(Pid, PidRef, Timeout,
                                         IbrowseRef, false)}
            after Timeout ->
                    Pid ! {PidRef, {error, timeout}},
                    throw({error, {ibrowse, timeout}})
            end
    end.

%% -------------------------------------------------------------------
%%
%% riakhttpc: Riak HTTP Client
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
%%      encode and decode secondary index queries.
-module(rhc_index).

-include_lib("riakc/include/riakc.hrl").
-include("raw_http.hrl").
-export([
         query_options/1,
         wait_for_index/1,
         index_acceptor/2
        ]).

query_options(Options) ->
    lists:flatmap(fun query_option/1, Options).

query_option({timeout, N}) when is_integer(N) ->
    [{?Q_TIMEOUT, integer_to_list(N)}];
query_option({stream, B}) when is_boolean(B) ->
    [{?Q_STREAM, atom_to_list(B)}];
query_option({max_results, N}) when is_integer(N) ->
    [{?Q_MAXRESULTS, integer_to_list(N)}];
query_option({continuation, C}) when is_binary(C) ->
    [{?Q_CONTINUATION, binary_to_list(C)}];
query_option({continuation, C}) when is_list(C) ->
    [{?Q_CONTINUATION, C}];
query_option({return_terms, B}) when is_boolean(B) ->
    [{?Q_RETURNTERMS, atom_to_list(B)}];
query_option(_) ->
    [].


wait_for_index(ReqId) ->
    wait_for_index(ReqId, []).

wait_for_index(ReqId, Acc) ->
    receive
        {ReqId, {done, Continuation}} ->
            {ok, collect_results(Acc, Continuation)};
        {ReqId, done} ->
            {ok, collect_results(Acc, undefined)};
        {ReqId, {error, Reason}} ->
            {error, Reason};
        {ReqId, ?INDEX_STREAM_RESULT{}=Res} ->
            wait_for_index(ReqId, [Res|Acc])
    end.

collect_results(Acc, Continuation) ->
    lists:foldr(fun merge_index_results/2,
                ?INDEX_RESULTS{keys=[],
                               terms=[],
                               continuation=Continuation}, Acc).

merge_index_results(?INDEX_STREAM_RESULT{keys=KL},
                    ?INDEX_RESULTS{keys=K0}=Acc) when is_list(KL) ->
    Acc?INDEX_RESULTS{keys=KL++K0};
merge_index_results(?INDEX_STREAM_RESULT{terms=TL},
                    ?INDEX_RESULTS{terms=T0}=Acc) when is_list(TL) ->
    Acc?INDEX_RESULTS{terms=TL++T0}.

index_acceptor(Pid, PidRef) ->
    receive
        {ibrowse_req_id, PidRef, IbrowseRef} ->
            index_acceptor(Pid, PidRef, IbrowseRef)
    end.

index_acceptor(Pid, PidRef, IBRef) ->
    receive
        {ibrowse_async_headers, IBRef, Status, Headers} ->
            case Status of
                "503" ->
                    Pid ! {PidRef, {error, timeout}};
                "200" ->
                    {"multipart/mixed", Args} = rhc_obj:ctype_from_headers(Headers),
                    Boundary = proplists:get_value("boundary", Args),
                    stream_parts_acceptor(
                      Pid, PidRef,
                      webmachine_multipart:stream_parts(
                        {[], stream_parts_helper(Pid, PidRef, IBRef, true)}, Boundary));
                _ ->
                    Pid ! {PidRef, {error, {Status, Headers}}}
            end
    end.

stream_parts_acceptor(Pid, PidRef, done_parts) ->
    Pid ! {PidRef, done};
stream_parts_acceptor(Pid, PidRef, {{_Name, _Param, Part},Next}) ->
    {struct, Response} = mochijson2:decode(Part),
    Keys = proplists:get_value(<<"keys">>, Response),
    Results = proplists:get_value(<<"results">>, Response),
    Continuation = proplists:get_value(<<"continuation">>, Response),
    maybe_send_results(Pid, PidRef, Keys, Results),
    maybe_send_continuation(Pid, PidRef, Continuation),
    stream_parts_acceptor(Pid, PidRef, Next()).

maybe_send_results(_Pid, _PidRef, undefined, undefined) -> ok;
maybe_send_results(Pid, PidRef, Keys, Results) ->
    Pid ! {PidRef, ?INDEX_STREAM_RESULT{keys=Keys,
                                        terms=Results}}.

maybe_send_continuation(_Pid, _PidRef, undefined) -> ok;
maybe_send_continuation(Pid, PidRef, Continuation) ->
            Pid ! {PidRef, {done, Continuation}}.

%% @doc "next" fun for the webmachine_multipart streamer - waits for
%%      an ibrowse message, and then returns it to the streamer for processing
stream_parts_helper(Pid, PidRef, IbrowseRef, First) ->
    fun() ->
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
                     stream_parts_helper(Pid, PidRef, IbrowseRef, false)}
            end
    end.

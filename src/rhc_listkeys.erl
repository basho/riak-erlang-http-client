%% @doc This module contains utilities that the rhc module uses to
%%      parse list_keys request results.
-module(rhc_listkeys).

-export([wait_for_listkeys/2]).
%% spawnable exports
-export([list_keys_acceptor/2]).

-include("raw_http.hrl").
-include("rhc.hrl").

%% @doc Collect all keylist results, and provide them as one list
%%      instead of streaming to a Pid.
%% @spec wait_for_listkeys(term(), integer()) ->
%%            {ok, [key()]}|{error, term()}
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

%% @doc first stage of ibrowse response handling - just waits to be
%%      told what ibrowse request ID to expect
list_keys_acceptor(Pid, PidRef) ->
    receive
        {ibrowse_req_id, PidRef, IbrowseRef} ->
            list_keys_acceptor(Pid,PidRef,IbrowseRef,[])
    after ?DEFAULT_TIMEOUT ->
            Pid ! {PidRef, {error, {timeout, []}}}
    end.

%% @doc main loop for ibrowse response handling - parses response and
%%      sends messaged to client Pid
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

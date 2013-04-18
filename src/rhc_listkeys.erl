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
%%      parse list_keys request results.
-module(rhc_listkeys).

-export([wait_for_list/2]).
%% spawnable exports
-export([list_acceptor/2]).

-include("raw_http.hrl").
-include("rhc.hrl").

-record(parse_state, {buffer=[],    %% unused characters in reverse order
                      brace=0,      %% depth of braces in current partial
                      quote=false,  %% inside a quoted region?
                      escape=false  %% last character was escape?
                     }).

%% @doc Collect all keylist results, and provide them as one list
%%      instead of streaming to a Pid.
%% @spec wait_for_list(term(), integer()) ->
%%            {ok, [key()]}|{error, term()}
wait_for_list(ReqId, Timeout) ->
    wait_for_list(ReqId,Timeout,[]).
%% @private
wait_for_list(ReqId, _Timeout0, Acc) ->
    receive
        {ReqId, done} -> 
            {ok, lists:flatten(Acc)};
        {ReqId, {error, Reason}} -> 
            {error, Reason};
        {ReqId, {_,Res}} -> 
            wait_for_list(ReqId,_Timeout0,[Res|Acc])
    end.

%% @doc first stage of ibrowse response handling - just waits to be
%%      told what ibrowse request ID to expect
list_acceptor(Pid, PidRef) ->
    receive
        {ibrowse_req_id, PidRef, IbrowseRef} ->
            list_acceptor(Pid,PidRef,IbrowseRef,#parse_state{})
    end.

%% @doc main loop for ibrowse response handling - parses response and
%%      sends messaged to client Pid
list_acceptor(Pid,PidRef,IbrowseRef,ParseState) ->
    receive
        {ibrowse_async_response_end, IbrowseRef} ->
            case is_empty(ParseState) of
                true ->
                    Pid ! {PidRef, done};
                false ->
                    Pid ! {PidRef, {error,
                                    {not_parseable,
                                     ParseState#parse_state.buffer}}}
            end;
        {ibrowse_async_response, IbrowseRef, {error,Error}} ->
            Pid ! {PidRef, {error, Error}};
        {ibrowse_async_response, IbrowseRef, []} ->
            %% ignore empty data
            ibrowse:stream_next(IbrowseRef),
            list_acceptor(Pid,PidRef,IbrowseRef,ParseState);
        {ibrowse_async_response, IbrowseRef, Data} ->
                try
                    {Keys, NewParseState} = try_parse(Data, ParseState),
                    if Keys =/= [] -> Pid ! {PidRef, {keys, Keys}};
                       true        -> ok
                    end,
                    list_acceptor(Pid, PidRef, IbrowseRef, NewParseState)
                catch
                    Error ->
                        Pid ! {PidRef, {error, Error}}
                end;
        {ibrowse_async_headers, IbrowseRef, Status, Headers} ->
            if Status =/= "200" ->
                    Pid ! {PidRef, {error, {Status, Headers}}};
               true ->
                    ibrowse:stream_next(IbrowseRef),
                    list_acceptor(Pid,PidRef,IbrowseRef,ParseState)
            end
    end.

is_empty(#parse_state{buffer=[],brace=0,quote=false,escape=false}) ->
    true;
is_empty(#parse_state{}) ->
    false.

try_parse(Data, #parse_state{buffer=B, brace=D, quote=Q, escape=E}) ->
    Parse = try_parse(binary_to_list(Data), B, D, Q, E),
    {KeyLists, NewParseState} =
        lists:foldl(
          fun(Chunk, Acc) when is_list(Chunk), is_list(Acc) ->
                  {struct, Props} =  mochijson2:decode(Chunk),
                  Keys = 
                      case proplists:get_value(<<"keys">>, Props, []) of
                          [] -> 
                              proplists:get_value(<<"buckets">>, 
                                                  Props, []);
                          K -> K
                      end,
                  case Keys of
                      [] -> 
                          %%check for a timeout error
                          case proplists:get_value(<<"error">>, Props, []) of
                              [] -> Acc;
                              Err -> throw(Err)
                          end;
                      _ -> [Keys|Acc]
                  end;
             (PS=#parse_state{}, Acc) ->
                  {Acc,PS}
          end,
          [],
          Parse),
    {lists:flatten(KeyLists), NewParseState}.

try_parse([], B, D, Q, E) ->
    [#parse_state{buffer=B, brace=D, quote=Q, escape=E}];
try_parse([_|Rest],B,D,Q,true) ->
    try_parse(Rest,B,D,Q,false);
try_parse([92|Rest],B,D,Q,false) -> %% backslash
    try_parse(Rest,B,D,Q,true);
try_parse([34|Rest],B,D,Q,E) -> %% quote
    try_parse(Rest,[34|B],D,not Q,E);
try_parse([123|Rest],B,D,Q,E) -> %% open brace
    if Q    -> try_parse(Rest,[123|B],D,Q,E);
       true -> try_parse(Rest,[123|B],D+1,Q,E)
    end;
try_parse([125|Rest],B,D,Q,E) -> %% close brace
    if Q    -> try_parse(Rest,B,D,Q,E);
       true ->
            if D == 1 -> %% end of a chunk
                    [lists:reverse([125|B])
                     |try_parse(Rest,[],0,Q,E)];
               true ->
                    try_parse(Rest,[125|B],D-1,Q,E)
            end
    end;
try_parse([C|Rest],B,D,Q,E) ->
    try_parse(Rest,[C|B],D,Q,E).

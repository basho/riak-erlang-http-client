%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at

%%   http://www.apache.org/licenses/LICENSE-2.0

%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% Constants used by the raw_http resources
%% original source at
%% http://github.com/basho/riak_kv/blob/master/src/riak_kv_wm_raw.hrl

%%======================================================================
%% Names of riak_object metadata fields
%%======================================================================
-define(MD_CTYPE,    <<"content-type">>).
-define(MD_CHARSET,  <<"charset">>).
-define(MD_ENCODING, <<"content-encoding">>).
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LINKS,    <<"Links">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(MD_USERMETA, <<"X-Riak-Meta">>).
-define(MD_INDEX,    <<"index">>).

%%======================================================================
%% Names of HTTP header fields
%%======================================================================
-define(HEAD_CTYPE,           "Content-Type").
-define(HEAD_VCLOCK,          "X-Riak-Vclock").
-define(HEAD_LINK,            "Link").
-define(HEAD_ENCODING,        "Content-Encoding").
-define(HEAD_CLIENT,          "X-Riak-ClientId").
-define(HEAD_USERMETA_PREFIX, "x-riak-meta-").
-define(HEAD_INDEX_PREFIX,    "X-Riak-Index-").
-define(HEAD_IF_NOT_MODIFIED, "X-Riak-If-Not-Modified").

%%======================================================================
%% JSON keys/values
%%======================================================================

%% Top-level keys in JSON bucket responses
-define(JSON_PROPS,      <<"props">>).
-define(JSON_KEYS,       <<"keys">>).

%% Names of JSON fields in bucket properties
-define(JSON_ALLOW_MULT, <<"allow_mult">>).
-define(JSON_BACKEND,    <<"backend">>).
-define(JSON_BASIC_Q,    <<"basic_quorum">>).
-define(JSON_BIG_VC,     <<"big_vclock">>).
-define(JSON_CHASH,      <<"chash_keyfun">>).
-define(JSON_DW,         <<"dw">>).
-define(JSON_LINKFUN,    <<"linkfun">>).
-define(JSON_LWW,        <<"last_write_wins">>).
-define(JSON_NF_OK,      <<"notfound_ok">>).
-define(JSON_N_VAL,      <<"n_val">>).
-define(JSON_OLD_VC,     <<"old_vclock">>).
-define(JSON_POSTCOMMIT, <<"postcommit">>).
-define(JSON_PR,         <<"pr">>).
-define(JSON_PRECOMMIT,  <<"precommit">>).
-define(JSON_PW,         <<"pw">>).
-define(JSON_R,          <<"r">>).
-define(JSON_REPL,       <<"repl">>).
-define(JSON_RW,         <<"rw">>).
-define(JSON_SEARCH,     <<"search">>).
-define(JSON_SMALL_VC,   <<"small_vclock">>).
-define(JSON_W,          <<"w">>).
-define(JSON_YOUNG_VC,   <<"young_vclock">>).

%% Valid quorum property values in JSON
-define(JSON_ALL,        <<"all">>).
-define(JSON_ONE,        <<"one">>).
-define(JSON_QUORUM,     <<"quorum">>).

%% Valid 'repl' property values in JSON
-define(JSON_BOTH,       <<"both">>).
-define(JSON_FULLSYNC,   <<"fullsync">>).
-define(JSON_REALTIME,   <<"realtime">>).

%% MapReduce JSON fields
-define(JSON_MOD,        <<"mod">>). %% also used by bucket props
-define(JSON_FUN,        <<"fun">>). %% also used by bucket props
-define(JSON_JSANON,     <<"jsanon">>).
-define(JSON_JSBUCKET,   <<"bucket">>).
-define(JSON_JSFUN,      <<"jsfun">>).
-define(JSON_JSKEY,      <<"key">>).

%% 2i fields
-define(JSON_RESULTS,      <<"results">>).
-define(JSON_CONTINUATION, <<"continuation">>).

%% dt related-fields
-define(JSON_HLL_PRECISION, <<"hll_precision">>).

%%======================================================================
%% Names of HTTP query parameters
%%======================================================================
-define(Q_PROPS, "props").
-define(Q_KEYS,  "keys").
-define(Q_BUCKETS,  "buckets").
-define(Q_FALSE, "false").
-define(Q_TRUE, "true").
-define(Q_TIMEOUT, "timeout").
-define(Q_STREAM, "stream").
-define(Q_VTAG,  "vtag").
-define(Q_RETURNBODY, "returnbody").
-define(Q_RETURNTERMS, "return_terms").
-define(Q_PAGINATION_SORT, "pagination_sort").
-define(Q_MAXRESULTS, "max_results").
-define(Q_CONTINUATION, "continuation").
-define(Q_TERM_REGEX, "term_regex").

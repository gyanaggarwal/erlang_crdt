%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Copyright (c) 2016 Gyanendra Aggarwal.  All Rights Reserved.
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
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(erlang_crdt).

-export([start/0, 
         stop/0,
	 setup_repl/1,
	 mutate/2,
	 query/2,
	 stop/1,
	 resume/2,
	 pretty_query/2]).

-include("erlang_crdt.hrl").

start() ->
    application:start(erlang_crdt).

stop() ->
    application:stop(erlang_crdt).

setup_repl(NodeList) ->
    gen_server:abcast(NodeList, ?EC_CRDT_SERVER, {?EC_MSG_SETUP_REPL, NodeList}).

mutate(Node, Ops) ->
    case gen_server:call({?EC_CRDT_SERVER, Node}, {?EC_MSG_CAUSAL_CONTEXT, Ops}) of
	{error, Reason} ->
	    {error, Reason};
	DL              ->
	    gen_server:call({?EC_CRDT_SERVER, Node}, {?EC_MSG_MUTATE, {Ops, DL}})
    end.

resume(Node, NodeList) ->    
    gen_server:call({?EC_CRDT_SERVER, Node}, {?EC_MSG_RESUME, NodeList}).

query(Node, Criteria) ->
    gen_server:call({?EC_CRDT_SERVER, Node}, {?EC_MSG_QUERY, Criteria}).

% only for demo

stop(Node) ->
    gen_server:cast({?EC_CRDT_SERVER, Node}, {stop, normal}).

pretty_query(Node, Criteria) ->
    ec_sets_util:pretty(query(Node, Criteria)).

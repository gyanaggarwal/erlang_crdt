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

-module(ec_crdt_server).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, 
	 handle_call/3, 
	 handle_cast/2, 
	 handle_info/2, 
	 code_change/3, 
	 terminate/2]).

-include("erlang_crdt.hrl").

start_link(AppConfig) ->
    gen_server:start_link({local, ?EC_CRDT_SERVER}, ?MODULE, [AppConfig], []).

init([AppConfig]) ->
    State = #ec_crdt_state{state_dvv = ec_gen_crdt:new(?EC_COMPMAP, ?EC_UNDEFINED),
			   delta_dvv = ec_gen_crdt:new(?EC_COMPMAP, ?EC_UNDEFINED),
			   app_config=AppConfig},
    {ok, State}.

handle_call(?EC_MSG_STOP,
	    _From,
	    #ec_crdt_state{}=State) ->
    print(handle_call, ?EC_MSG_STOP),
    {reply, 
     ok, 
     State#ec_crdt_state{status=?EC_INACTIVE}};
handle_call(?EC_MSG_RESUME,
	    _From,
	    #ec_crdt_state{app_config=AppConfig}=State) ->
    print(handle_call, ?EC_MSG_RESUME),
    {reply, 
     ok, 
     State#ec_crdt_state{status=?EC_ACTIVE, timeout_start=ec_time_util:get_current_time()}, 
     ec_crdt_config:get_timeout_period(AppConfig)};
handle_call({?EC_MSG_CAUSAL_CONTEXT, Ops}, 
	    _From,
	    #ec_crdt_state{status=?EC_ACTIVE, state_dvv=StateDvv}=State) ->
    print(handle_call, ?EC_MSG_CAUSAL_CONTEXT, Ops),
    {reply, 
     ec_gen_crdt:causal_context(Ops, StateDvv), 
     State, 
     get_timeout(State)};
handle_call({?EC_MSG_MUTATE, {Ops, DL}},
	    _From,
	    #ec_crdt_state{status=?EC_ACTIVE, state_dvv=StateDvv, delta_dvv=DeltaDvv, app_config=AppConfig}=State) ->
    print(handle_call, ?EC_MSG_MUTATE, {Ops, DL}),
    {Reply, State1} = case ec_gen_crdt:mutate(Ops, DL, DeltaDvv, StateDvv, ec_crdt_config:get_node_id(AppConfig)) of
			  {ok, DeltaDvv1, StateDvv1} ->
			      {ok, State#ec_crdt_state{state_dvv=StateDvv1, delta_dvv=DeltaDvv1}};
			  {error, Reason}            ->
			      {{error, Reason}, State}
		      end,
    {reply, 
     Reply, 
     State1, 
     get_timeout(State1)};
handle_call({?EC_MSG_QUERY, Criteria},
	    _From,
	    #ec_crdt_state{status=?EC_ACTIVE, state_dvv=StateDvv}=State) ->
    print(handle_call, ?EC_MSG_QUERY, Criteria),
    {reply, 
     ec_gen_crdt:query(Criteria, StateDvv), 
     State, 
     get_timeout(State)};
handle_call(_Msg,
            _From, 
            #ec_crdt_state{}=State) ->
    {reply, 
     {error, ?EC_INVALID_OPERATION}, 
     State}.

handle_cast({stop, Reason}, 
	    #ec_crdt_state{}=State) ->
    {stop, Reason, State};
handle_cast({?EC_MSG_SETUP_REPL, NodeList}, 
	    #ec_crdt_state{app_config=AppConfig}=State) ->
    print(handle_cast, ?EC_MSG_SETUP_REPL, NodeList),
    {noreply, 
     State#ec_crdt_state{status=?EC_ACTIVE,
			 timeout_start=ec_time_util:get_current_time(), 
			 replica_cluster=lists:delete(ec_crdt_config:get_node_id(AppConfig), NodeList)}, 
     ec_crdt_config:get_timeout_period(AppConfig)};
handle_cast({?EC_MSG_MERGE, {SenderNodeId, DeltaList}},
	    #ec_crdt_state{status=?EC_ACTIVE, state_dvv=StateDvv, app_config=AppConfig}=State) ->
    print(handle_cast, ?EC_MSG_MERGE, {SenderNodeId, DeltaList}),
    NodeId = ec_crdt_config:get_node_id(AppConfig),
    StateDvv1 = lists:foldl(fun(DeltaDvvx, StateDvvx) -> merge_fun(NodeId, SenderNodeId, DeltaDvvx, StateDvvx) end, StateDvv, DeltaList),
    State1 = State#ec_crdt_state{state_dvv=StateDvv1},
    {noreply, 
     State1, 
     get_timeout(State1)};
handle_cast({?EC_MSG_CAUSAL, {SenderNodeId, #ec_dvv{}=CausalDvv}},
	    #ec_crdt_state{status=?EC_ACTIVE, state_dvv=StateDvv, app_config=AppConfig}=State) ->
    print(handle_cast, ?EC_MSG_CAUSAL, {SenderNodeId, CausalDvv}),
    NodeId = ec_crdt_config:get_node_id(AppConfig),
    case ec_gen_crdt:causal_consistent(CausalDvv, StateDvv, SenderNodeId, ?EC_GLOBAL) of
	{error, [{?EC_CAUSALLY_AHEAD, _} | _]=CausalList} ->
	    ec_crdt_peer_api:delta_interval(SenderNodeId, NodeId, ec_crdt_util:causal_dvv(?EC_CAUSALLY_AHEAD, CausalList));
	_                                                 ->
	    ok
    end,
    {noreply, 
     State, 
     get_timeout(State)};
handle_cast({?EC_MSG_DELTA_INTERVAL, {SenderNodeId, CausalDvv}}, 
	    #ec_crdt_state{status=?EC_ACTIVE}=State) ->
    print(handle_cast, ?EC_MSG_DELTA_INTERVAL, {SenderNodeId, CausalDvv}),
%to_do
    {noreply, 
     State, 
     get_timeout(State)};
handle_cast(_Msg, 
	    #ec_crdt_state{}=State) ->
    {noreply, State}.

handle_info(timeout, 
	    #ec_crdt_state{status=?EC_ACTIVE, replica_cluster=ReplicaCluster, state_dvv=StateDvv, delta_dvv=DeltaDvv, app_config=AppConfig}=State) ->
    print(handle_info, timeout),
    NodeId = ec_crdt_config:get_node_id(AppConfig),
    DeltaDvv1 = case ec_crdt_util:is_dirty(DeltaDvv) of
		    true  ->
			ec_crdt_peer_api:merge(ReplicaCluster, NodeId, ec_gen_crdt:mutated(DeltaDvv)),
			ec_gen_crdt:reset(DeltaDvv, NodeId);
		    false ->
			ec_crdt_peer_api:causal(ReplicaCluster, NodeId, ec_gen_crdt:causal_history(StateDvv, NodeId)),
			DeltaDvv
	        end,
    {noreply, 
     State#ec_crdt_state{delta_dvv=DeltaDvv1, 
			 timeout_start=ec_time_util:get_current_time()},
     ec_crdt_config:get_timeout_period(AppConfig)};
handle_info(_Msg, 
	    #ec_crdt_state{}=State) ->
    {noreply, State}.

code_change(_OldVsn, #ec_crdt_state{}=State, _Extra) ->
    {ok, State}.

terminate(_Reason, #ec_crdt_state{}) ->
    ok.

% private function

get_timeout(#ec_crdt_state{timeout_start=TimeoutStart, app_config=AppConfig}) ->
    ec_time_util:get_timeout(TimeoutStart, ec_crdt_config:get_timeout_period(AppConfig)).

merge_fun(NodeId, SenderNodeId, DeltaDvv, StateDvv) ->
    case ec_gen_crdt:merge(DeltaDvv, StateDvv,SenderNodeId) of
	{ok, StateDvv1}                                   ->
	    StateDvv1;
        {error, [{?EC_CAUSALLY_AHEAD, _} | _]=CausalList} ->
            ec_crdt_peer_api:delta_interval(SenderNodeId, NodeId, ec_crdt_util:causal_dvv(?EC_CAUSALLY_AHEAD, CausalList)),
            StateDvv;
	 _                                                ->
            StateDvv
    end.

print(Tag, MsgTag, Msg) ->
    io:fwrite("~p, ~p, ~p~n", [Tag, MsgTag, Msg]).

print(Tag, MsgTag) ->
    io:fwrite("~p, ~p~n", [Tag, MsgTag]).


      

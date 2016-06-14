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
    random:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
    State = #ec_crdt_state{timeout_period = ec_time_util:get_random(ec_crdt_config:get_timeout_period(AppConfig)),
			   state_dvv      = ec_gen_crdt:new(?EC_COMPMAP, ?EC_UNDEFINED),
			   delta_dvv      = ec_gen_crdt:new(?EC_COMPMAP, ?EC_UNDEFINED),
			   delta_interval = ec_gen_crdt:new(?EC_COMPMAP, ?EC_UNDEFINED),
			   app_config     = AppConfig},
    {ok, State}.

handle_call(?EC_MSG_STOP,
	    _From,
	    #ec_crdt_state{}=State) ->
    print(stopped),
    {reply, 
     ok, 
     State#ec_crdt_state{last_msg=?EC_MSG_STOP, 
			 status=?EC_INACTIVE}};
handle_call(?EC_MSG_RESUME,
	    _From,
	    #ec_crdt_state{timeout_period=TimeoutPeriod}=State) ->
    print(resumed),
    {reply, 
     ok, 
     State#ec_crdt_state{last_msg=?EC_MSG_RESUME, 
			 status=?EC_ACTIVE, 
			 timeout_start=ec_time_util:get_current_time()}, 
     TimeoutPeriod};
handle_call({?EC_MSG_CAUSAL_CONTEXT, Ops}, 
	    _From,
	    #ec_crdt_state{status=?EC_ACTIVE, 
			   state_dvv=StateDvv}=State) ->
    {reply, 
     ec_gen_crdt:causal_context(Ops, StateDvv), 
     State, 
     get_timeout(State)};
handle_call({?EC_MSG_MUTATE, {Ops, DL}},
	    _From,
	    #ec_crdt_state{last_msg=LastMsg, 
			   status=?EC_ACTIVE, 
			   state_dvv=StateDvv, 
			   delta_dvv=DeltaDvv,
			   delta_interval=DeltaInterval, 
			   app_config=AppConfig}=State) ->
    print_mutate(Ops, DeltaInterval#ec_dvv.di_num, LastMsg),
    NodeId = ec_crdt_config:get_node_id(AppConfig),
    {Reply, State1} = case ec_gen_crdt:mutate(Ops, DL, DeltaDvv, DeltaInterval, StateDvv, NodeId) of
			  {ok, {DeltaDvv1, DeltaInterval1, StateDvv1}} ->
			      DataManager = ec_crdt_config:get_data_manager(AppConfig),
			      DataManager:write_delta_mutation(ec_gen_crdt:mutated(DeltaDvv1)),
			      {ok, State#ec_crdt_state{state_dvv=StateDvv1, delta_dvv=ec_gen_crdt:reset(DeltaDvv1, NodeId), delta_interval=DeltaInterval1}};
			  {error, Reason}            ->
			      {{error, Reason}, State}
		      end,
    {reply, 
     Reply, 
     State1#ec_crdt_state{last_msg=?EC_MSG_MUTATE}, 
     get_timeout(State1)};
handle_call({?EC_MSG_QUERY, Criteria},
	    _From,
	    #ec_crdt_state{status=?EC_ACTIVE, 
			   state_dvv=StateDvv}=State) ->
    {reply, 
     ec_gen_crdt:query(Criteria, StateDvv), 
     State#ec_crdt_state{last_msg=?EC_MSG_QUERY}, 
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
	    #ec_crdt_state{timeout_period=TimeoutPeriod,
			   app_config=AppConfig}=State) ->
    {noreply, 
     State#ec_crdt_state{last_msg=?EC_MSG_SETUP_REPL,
			 status=?EC_ACTIVE,
			 timeout_start=ec_time_util:get_current_time(), 
			 replica_cluster=lists:delete(ec_crdt_config:get_node_id(AppConfig), NodeList)}, 
     TimeoutPeriod};
handle_cast({?EC_MSG_MERGE, {SenderNodeId, DeltaList}},
	    #ec_crdt_state{last_msg=LastMsg, 
			   status=?EC_ACTIVE, 
			   state_dvv=StateDvv,
			   app_config=AppConfig}=State) ->
    print_merge(SenderNodeId, get_di_num_list(DeltaList), LastMsg),
    DataManager = ec_crdt_config:get_data_manager(AppConfig),
    StateDvv1 = lists:foldl(fun(DeltaDvvx, StateDvvx) -> merge_fun(SenderNodeId, DeltaDvvx, StateDvvx, DataManager) end, StateDvv, DeltaList),
    State1 = State#ec_crdt_state{state_dvv=StateDvv1},
    {noreply, 
     State1#ec_crdt_state{last_msg=?EC_MSG_MERGE}, 
     get_timeout(State1)};
handle_cast({?EC_MSG_CAUSAL_HISTORY, {SenderNodeId, #ec_dvv{}=CausalHistory}},
	    #ec_crdt_state{status=?EC_ACTIVE, 
			   app_config=AppConfig}=State) ->
    NodeId = ec_crdt_config:get_node_id(AppConfig),
    LocalCausalHistory = ec_gen_crdt:causal_history(CausalHistory, NodeId),
    DataManager = ec_crdt_config:get_data_manager(AppConfig),
    case DataManager:read_delta_interval(LocalCausalHistory, NodeId) of
	[]     ->
	    ok;
	DIList ->
	    ec_crdt_peer_api:merge([SenderNodeId], NodeId, DIList)
     end,
    {noreply, 
     State#ec_crdt_state{last_msg=?EC_MSG_CAUSAL_HISTORY}, 
     get_timeout(State)};
handle_cast(_Msg, 
	    #ec_crdt_state{}=State) ->
    {noreply, State}.

handle_info(timeout, 
	    #ec_crdt_state{status=?EC_ACTIVE,
			   timeout_period=TimeoutPeriod,
			   replica_cluster=ReplicaCluster, 
			   state_dvv=StateDvv, 
			   delta_interval=DeltaInterval, 
			   app_config=AppConfig}=State) ->
    NodeId = ec_crdt_config:get_node_id(AppConfig),
    DeltaInterval1 = case ec_crdt_util:is_dirty(DeltaInterval) of
			 true  ->
			     MDeltaInterval = ec_gen_crdt:mutated(DeltaInterval),
			     DataManager = ec_crdt_config:get_data_manager(AppConfig),
			     DataManager:write_delta_interval(MDeltaInterval),
			     ec_crdt_peer_api:merge(ReplicaCluster, NodeId, [MDeltaInterval]),
			     ec_gen_crdt:reset(DeltaInterval, NodeId);
			 false ->
			     ec_crdt_peer_api:causal_history(ReplicaCluster, NodeId, ec_gen_crdt:causal_history(StateDvv, ?EC_UNDEFINED)),
			     DeltaInterval
	             end,
    {noreply, 
     State#ec_crdt_state{delta_interval=DeltaInterval1, 
			 timeout_start=ec_time_util:get_current_time()},
     TimeoutPeriod};
handle_info(_Msg, 
	    #ec_crdt_state{}=State) ->
    {noreply, State}.

code_change(_OldVsn, #ec_crdt_state{}=State, _Extra) ->
    {ok, State}.

terminate(_Reason, #ec_crdt_state{}) ->
    ok.

% private function

get_timeout(#ec_crdt_state{timeout_start=TimeoutStart, timeout_period=TimeoutPeriod}) ->
    ec_time_util:get_timeout(TimeoutStart, TimeoutPeriod).

merge_fun(SenderNodeId, DeltaInterval, StateDvv, DataManager) ->
    case ec_gen_crdt:merge(DeltaInterval, StateDvv, SenderNodeId) of
	{ok, StateDvv1} ->
	    DataManager:write_delta_mutation(DeltaInterval),
	    StateDvv1;
	 _Other         ->
            StateDvv
    end.

get_di_num_list(DIList) ->    
    lists:map(fun(DVV) -> DVV#ec_dvv.di_num end, DIList).

print_mutate(Msg, DINum, LastMsg) ->
    case LastMsg =:= ?EC_MSG_MUTATE of
	true  ->
	    io:fwrite("   mutate   ~p delta_interval=[~p]~n", [Msg, DINum]);
	false ->
	    io:fwrite("~n   mutate   ~p delta_interval=[~p]~n", [Msg, DINum])
    end.

print_merge(Msg, NumList, LastMsg) ->
    case LastMsg =:= ?EC_MSG_MERGE of
	true  ->
	    io:fwrite("   merge    ~p delta_interval=~p~n", [Msg, NumList]);
	false ->
	    io:fwrite("~n   merge    ~p delta_interval=~p~n", [Msg, NumList])
    end.

print(MsgTag) ->
    io:fwrite("~n   ~p~n", [MsgTag]).

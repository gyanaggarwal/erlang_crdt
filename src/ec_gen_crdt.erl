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

-module(ec_gen_crdt).

-include("erlang_crdt.hrl").

-callback new_crdt(Type :: atom(), 
		   Name :: term()) -> #ec_dvv{}.

-callback delta_crdt(Ops :: term(), 
		     DL :: list(), 
		     State :: #ec_dvv{}, 
		     ServerId :: term()) -> #ec_dvv{}.

-callback reconcile_crdt(State :: #ec_dvv{}, 
			 ServerId :: term(), 
			 Flag :: ?EC_LOCAL | ?EC_GLOBAL, 
			 DataStatus :: term()) -> #ec_dvv{}.

-callback update_fun_crdt(Args :: list()) -> {fun(), fun()}.

-callback merge_fun_crdt(Args :: list()) -> {fun(), fun()}.

-callback causal_consistent_crdt(Delta :: #ec_dvv{}, 
				 State :: #ec_dvv{}, 
				 ServerId :: term(),
				 Flag :: ?EC_LOCAL | ?EC_GLOBAL,
				 List :: list()) -> list().
       
-callback query_crdt(Criteria :: term(), 
		     State :: #ec_dvv{}) -> term().

-callback reset_crdt(State :: #ec_dvv{}, 
		     ServerId :: term()) -> #ec_dvv{}.

-callback mutated_crdt(DVV :: #ec_dvv{}) -> #ec_dvv{}.

-callback causal_context_crdt(Ops :: term(), 
			      State :: #ec_dvv{}) -> list().

-callback causal_history_crdt(State :: #ec_dvv{},
			      ServerId :: term(),
			      Flag :: ?EC_CAUSAL_SERVER_ONLY | ?EC_CAUSAL_EXCLUDE_SERVER) -> #ec_dvv{}.

-callback change_status_crdt(State :: #ec_dvv{},
			     Status :: term()) -> #ec_dvv{}.
     

-export([new/2,
	 merge/2,
	 merge/3,
	 mutate/6,
	 mutated/1,
	 query/1,
	 query/2,
	 update/4,
	 causal_context/2,
	 reset/2,
	 causal_consistent/4,
	 causal_history/3,
	 change_status/2]).

-spec new(Type :: atom(), Name :: term()) -> #ec_dvv{}.
new(Type, Name) ->
    Mod = ec_crdt_util:find_module(Type),
    Mod:new_crdt(Type, Name).

-spec causal_context(Ops :: term(), State :: #ec_dvv{}) -> list().
causal_context(Ops, #ec_dvv{module=Mod}=State) ->
    Mod:causal_context_crdt(Ops, State).

-spec causal_history(State :: #ec_dvv{}, ServerId :: term(), Flag :: ?EC_CAUSAL_SERVER_ONLY | ?EC_CAUSAL_EXCLUDE_SERVER) -> #ec_dvv{}.
causal_history(#ec_dvv{module=Mod}=State, ServerId, Flag) ->
    Mod:causal_history_crdt(State, ServerId, Flag).

-spec change_status(DVV :: #ec_dvv{}, Status :: term()) -> #ec_dvv{}.
change_status(#ec_dvv{module=Mod}=DVV, Status) ->
    Mod:change_status_crdt(DVV, Status).

-spec causal_consistent(Delta ::#ec_dvv{}, State :: #ec_dvv{}, ServerId :: term(), Flag :: ?EC_LOCAL | ?EC_GLOBAL) -> list().
causal_consistent(#ec_dvv{module=Mod, type=Type, name=Name}=Delta,
		  #ec_dvv{module=Mod, type=Type, name=Name}=State,
		  ServerId,
		  Flag) ->
    Mod:causal_consistent_crdt(Delta, State, ServerId, Flag, []).

-spec merge(Delta :: #ec_dvv{}, State :: #ec_dvv{}) -> {ok, #ec_dvv{}} | {error, atom() | {atom(), #ec_dot{}}}.
merge(#ec_dvv{module=Mod, type=Type, name=Name, dot_list=[#ec_dot{replica_id=ServerId}]}=Delta,
      #ec_dvv{module=Mod, type=Type, name=Name}=State) ->
    merge(Delta, State, ServerId);
merge(#ec_dvv{module=Mod, type=Type, name=Name, dot_list=[]}, 
      #ec_dvv{module=Mod, type=Type, name=Name}) ->
    {error, ?EC_EMPTY_DELTA_INTERVAL};
merge(#ec_dvv{module=Mod, type=Type, name=Name}, 
      #ec_dvv{module=Mod, type=Type, name=Name}) ->
    {error, ?EC_INCORRECT_DELTA_INTERVAL}.

-spec merge(Delta :: #ec_dvv{}, State :: #ec_dvv{}, ServerId :: term()) -> {ok, #ec_dvv{}} | {error, atom()}.
merge(#ec_dvv{module=Mod, type=Type, name=Name}=Delta, 
      #ec_dvv{module=Mod, type=Type, name=Name}=State, 
      ServerId) ->
    {Delta1, State1} = ec_crdt_util:delta_state_pair({Delta, State}),
    case causal_consistent(Delta1, State1, ServerId, ?EC_GLOBAL) of
	[]     ->
	    State2 = ec_dvv:sync([Delta1, State1], Mod:merge_fun_crdt([Type])),
	    State3 = ec_crdt_util:add_param(State2, State1),
	    State4 = Mod:reconcile_crdt(State3, ServerId, ?EC_GLOBAL, ?EC_DVV_DIRTY_STATE),
	    {ok, change_status(ec_crdt_util:add_param(State4, State1), ?EC_DVV_DIRTY_STATE)};
	Reason ->
	    {error, Reason}
    end.

-spec mutate(Ops :: term(), 
	     DL :: list(),
	     DI10 :: #ec_dvv{},
	     DI20 :: #ec_dvv{}, 
	     State :: #ec_dvv{}, 
	     ServerId :: term()) -> {ok, {#ec_dvv{}, #ec_dvv{}, #ec_dvv{}}} | {error, atom()}.
mutate(Ops, 
       DL, 
       #ec_dvv{module=Mod, type=Type, name=Name}=DI10,
       #ec_dvv{module=Mod, type=Type, name=Name, di_num=DINum}=DI20, 
       #ec_dvv{module=Mod, type=Type, name=Name}=State, 
       ServerId) ->
    Delta = Mod:delta_crdt(Ops, DL, State, ServerId),
    case ec_crdt_util:is_dirty(Delta) of
	true  ->
	    case causal_consistent(Delta, State, ServerId, ?EC_LOCAL) of
		[]     ->
		    State1 = update(Delta, State, ServerId, ?EC_DVV_DIRTY_STATE),
		    DI11   = update(Delta, DI10,  ServerId, ?EC_DVV_DIRTY_DELTA),
		    DI21   = update(Delta, DI20,  ServerId, ?EC_DVV_DIRTY_DELTA),
		    {ok, {DI11, DI21#ec_dvv{di_num=DINum}, State1}};
		Reason ->
		    {error, Reason}
	    end;
	false  ->
	    {error, ?EC_INVALID_OPERATION}
    end.

-spec query(State :: #ec_dvv{}) -> {error, ?EC_INVALID_OPERATION} | term().
query(State) ->
    query(?EC_UNDEFINED, State).

-spec query(Criteria :: term(), State :: #ec_dvv{}) -> {error, ?EC_INVALID_OPERATION} | term().
query(Criteria, #ec_dvv{module=Mod}=State) ->
    Mod:query_crdt(Criteria, State).

-spec reset(DVV :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.
reset(#ec_dvv{module=Mod, di_num=DINum}=DVV, ServerId) ->
    DVV1 = Mod:reset_crdt(DVV, ServerId),
    DVV1#ec_dvv{status=?EC_DVV_CLEAN_DELTA, di_num=DINum+1}.
			
-spec mutated(DVV :: #ec_dvv{}) -> #ec_dvv{}.
mutated(#ec_dvv{module=Mod}=DVV) ->      
    Mod:mutated_crdt(DVV).

-spec update(Delta :: #ec_dvv{}, State :: #ec_dvv{}, ServerId :: term(), DataStatus :: term()) -> #ec_dvv{}.
update(#ec_dvv{module=Mod, type=Type, name=Name}=Delta, 
       #ec_dvv{module=Mod, type=Type, name=Name}=State, 
       ServerId,
       DataStatus) ->
    State1 = ec_dvv:update(Delta, State, Mod:update_fun_crdt([Type]), ServerId),
    State2 = ec_crdt_util:add_param(State1, State),
    State3 = Mod:reconcile_crdt(State2, ServerId, ?EC_LOCAL, DataStatus),
    ec_crdt_util:add_param(State3#ec_dvv{status=DataStatus}, State).



    

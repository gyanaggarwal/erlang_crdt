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

-callback new_crdt(Type :: atom(), Name :: term(), Args :: term()) -> #ec_dvv{}.

-callback delta_crdt(Ops :: term(), DL :: list(), State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.

-callback reconcile_crdt(State :: #ec_dvv{}, ServerId :: term(), Flag :: ?EC_RECONCILE_LOCAL | ?EC_RECONCILE_GLOBAL) -> #ec_dvv{}.

-callback update_fun_crdt(Args :: list()) -> {fun(), fun()}.

-callback merge_fun_crdt(Args :: list()) -> {fun(), fun()}.

-callback causal_consistent_crdt(Delta :: #ec_dvv{}, State :: #ec_dvv{}, Offset :: non_neg_integer(), ServerId :: term()) -> ?EC_CAUSALLY_CONSISTENT |
															     ?EC_CAUSALLY_BEHIND |
															     ?EC_CAUSALLY_AHEAD.
       
-callback query_crdt(Criteria :: term(), State :: #ec_dvv{}) -> term().

-callback reset_crdt(State :: #ec_dvv{}) -> #ec_dvv{}.

-callback mutated_crdt(DVV :: #ec_dvv{}) -> #ec_dvv{}.

-callback causal_list_crdt({Type :: atom(), Name :: term()} | ?EC_UNDEFINED, State :: #ec_dvv{}) -> list().

-export([new/2,
	 new/3,
	 merge/2,
	 merge/3,
	 mutate/5,
	 mutated/1,
	 query/1,
	 query/2,
	 causal_list/2,
	 reset/1]).

-spec new(Type :: atom(), Name :: term()) -> #ec_dvv{}.
new(Type, Name) ->
    new(Type, Name, ?EC_UNDEFINED).

-spec new(Type :: atom(), Name :: term(), Args :: term()) -> #ec_dvv{}.
new(Type, Name, Args) ->
    Mod = ec_crdt_util:find_module(Type),
    Mod:new_crdt(Type, Name, Args).

-spec causal_list({Type :: atom(), Name :: term()} | ?EC_UNDEFINED, State :: #ec_dvv{}) -> list().
causal_list(Args, #ec_dvv{module=Mod}=State) ->
    Mod:causal_list_crdt(Args, State).

-spec merge(Delta :: #ec_dvv{}, State :: #ec_dvv{}) -> {ok, #ec_dvv{}} | {error, atom() | {atom(), #ec_dot{}}}.
merge(#ec_dvv{module=Mod, type=Type, name=Name, option=Option, dot_list=[#ec_dot{replica_id=ServerId}]}=Delta,
      #ec_dvv{module=Mod, type=Type, name=Name, option=Option}=State) ->
    merge(Delta, State, ServerId);
merge(#ec_dvv{module=Mod, type=Type, name=Name, option=Option, dot_list=[]}, 
      #ec_dvv{module=Mod, type=Type, name=Name, option=Option}) ->
    {error, ?EC_EMPTY_DELTA_INTERVAL};
merge(#ec_dvv{module=Mod, type=Type, name=Name, option=Option}, 
      #ec_dvv{module=Mod, type=Type, name=Name, option=Option}) ->
    {error, ?EC_INCORRECT_DELTA_INTERVAL}.

-spec merge(Delta :: #ec_dvv{}, State :: #ec_dvv{}, ServerId :: term()) -> {ok, #ec_dvv{}} | {error, atom()}.
merge(#ec_dvv{module=Mod, type=Type, name=Name, option=Option}=Delta, 
      #ec_dvv{module=Mod, type=Type, name=Name, option=Option}=State, 
      ServerId) ->
    case Mod:causal_consistent_crdt(Delta, State, 1, ServerId) of
	?EC_CAUSALLY_CONSISTENT ->
	    State1 = ec_dvv:sync([Delta, State], Mod:merge_fun_crdt([Type])),
	    State2 = ec_crdt_util:add_param(State1, State),
	    State3 = Mod:reconcile_crdt(State2, ServerId, ?EC_RECONCILE_GLOBAL),
	    {ok, ec_crdt_util:add_param(State3#ec_dvv{status=?EC_DVV_DIRTY}, State)};
	?EC_CAUSALLY_AHEAD      ->
	    process_causally_ahead(State, ServerId);
	Reason                  ->
	    {error, Reason}
    end.

-spec mutate(Ops :: term(), DL :: list(), DI :: #ec_dvv{}, State :: #ec_dvv{}, ServerId :: term()) -> {ok, {#ec_dvv{}, #ec_dvv{}}} | {error, atom()}.
mutate(Ops, 
       DL, 
       #ec_dvv{module=Mod, type=Type, name=Name, option=Option}=DI, 
       #ec_dvv{module=Mod, type=Type, name=Name, option=Option}=State, 
       ServerId) ->
    UpdateFun = Mod:update_fun_crdt([Type]),
    Delta = Mod:delta_crdt(Ops, DL, State, ServerId),
    case ec_dvv:is_valid(Delta) of
	true  ->
	    Delta1 = Delta#ec_dvv{status=?EC_DVV_DIRTY},
	    case ec_dvv:causal_consistent(Delta1, State, 0, ServerId) of
		?EC_CAUSALLY_CONSISTENT ->
		    State1 = update(Delta1, State, UpdateFun, ServerId),
		    DI1    = update(Delta1, DI,    UpdateFun, ServerId),
		    {ok, DI1, State1};
		?EC_CAUSALLY_AHEAD      ->
		    process_causally_ahead(State, ServerId);
		Reason                  ->
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

-spec reset(DVV :: #ec_dvv{}) -> #ec_dvv{}.
reset(#ec_dvv{module=Mod}=DVV) ->
    DVV1 = Mod:reset_crdt(DVV),
    DVV1#ec_dvv{status=?EC_DVV_CLEAN}.
			
-spec mutated(DVV :: #ec_dvv{}) -> #ec_dvv{}.
mutated(#ec_dvv{module=Mod}=DVV) ->      
    Mod:mutated_crdt(DVV).

% private function

-spec update(Delta :: #ec_dvv{}, State :: #ec_dvv{}, UpdateFun :: {fun(), fun()}, ServerId :: term()) -> #ec_dvv{}.
update(#ec_dvv{module=Mod, type=Type, name=Name, option=Option}=Delta, 
       #ec_dvv{module=Mod, type=Type, name=Name, option=Option}=State, 
       UpdateFun, 
       ServerId) ->
    State1 = ec_dvv:update(Delta, State, UpdateFun, ServerId),
    State2 = ec_crdt_util:add_param(State1, State),
    State3 = Mod:reconcile_crdt(State2, ServerId, ?EC_RECONCILE_LOCAL),
    ec_crdt_util:add_param(State3#ec_dvv{status=?EC_DVV_DIRTY}, State).

-spec process_causally_ahead(State :: #ec_dvv{}, ServerId :: term()) -> {error, {?EC_CAUSALLY_AHEAD, atom() | #ec_dot{}}}.
process_causally_ahead(#ec_dvv{}=State, ServerId) ->
    Msg = case ec_crdt_util:find_dot(State, ServerId) of
	      false         ->
		  ?EC_DOT_DOES_NOT_EXIST;
	      #ec_dot{}=Dot ->
		  Dot#ec_dot{values=[]}
	  end,
    {error, {?EC_CAUSALLY_AHEAD, Msg}}.


    

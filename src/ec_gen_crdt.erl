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

-callback new_crdt(Type :: atom(), Args :: term()) -> #ec_dvv{}.

-callback delta_crdt(Ops :: term(), DL :: list(), State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.

-callback reconcile_crdt(State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.

-callback update_fun_crdt(Args :: list()) -> {fun(), fun()}.

-callback merge_fun_crdt(Args :: list()) -> {fun(), fun()}.

-callback causal_consistent_crdt(Delta :: #ec_dvv{}, State :: #ec_dvv{}, Offset :: non_neg_integer(), ServerId :: term()) -> ?EC_CAUSALLY_CONSISTENT |
															     ?EC_CAUSALLY_BEHIND |
															     ?EC_CAUSALLY_AHEAD.
       
-callback query_crdt(Criteria :: term(), State :: #ec_dvv{}) -> term().

-callback reset_crdt(State :: #ec_dvv{}) -> #ec_dvv{}.

-export([new/2,
	 new/3,
	 merge/2,
	 merge/3,
	 mutate/5,
	 query/1,
	 query/2,
	 reset/1]).

-spec new(Mod :: atom(), Type :: atom()) -> #ec_dvv{}.
new(Mod, Type) ->
    new(Mod, Type, undefined).

-spec new(Mod :: atom(), Type :: atom(), Args :: term()) -> #ec_dvv{}.
new(Mod, Type, Args) ->
    Mod:new_crdt(Type, Args).

-spec merge(Delta :: #ec_dvv{}, State :: #ec_dvv{}) -> {ok, #ec_dvv{}} | {error, atom() | {atom(), #ec_dot{}}}.
merge(#ec_dvv{module=Mod, type=Type, option=Option, dot_list=[#ec_dot{replica_id=ServerId}]}=Delta,
      #ec_dvv{module=Mod, type=Type, option=Option}=State) ->
    merge(Delta, State, ServerId);
merge(#ec_dvv{module=Mod, type=Type, option=Option, dot_list=[]}, 
      #ec_dvv{module=Mod, type=Type, option=Option}) ->
    {error, ?EC_EMPTY_DELTA_INTERVAL};
merge(#ec_dvv{module=Mod, type=Type, option=Option}, 
     #ec_dvv{module=Mod, type=Type, option=Option}) ->
    {error, ?EC_INCORRECT_DELTA_INTERVAL}.

-spec merge(Delta :: #ec_dvv{}, State :: #ec_dvv{}, ServerId :: term()) -> {ok, #ec_dvv{}} | {error, atom()}.
merge(#ec_dvv{module=Mod, type=Type, option=Option}=Delta, 
      #ec_dvv{module=Mod, type=Type, option=Option}=State, 
      ServerId) ->
    case Mod:causal_consistent_crdt(Delta, State, 1, ServerId) of
	?EC_CAUSALLY_CONSISTENT ->
	    State1 = ec_dvv:sync([Delta, State], Mod:merge_fun_crdt([Type])),
	    State2 = ec_crdt_util:add_param(State1, State),
	    State3 = Mod:reconcile_crdt(State2, ServerId),
	    {ok, ec_crdt_util:add_param(State3, State)};
	?EC_CAUSALLY_AHEAD      ->
	    process_causally_ahead(State, ServerId);
	Reason                  ->
	    {error, Reason}
    end.

-spec mutate(Ops :: term(), DL :: list(), DI :: #ec_dvv{}, State :: #ec_dvv{}, ServerId :: term()) -> {ok, {#ec_dvv{}, #ec_dvv{}}} | {error, atom()}.
mutate(Ops, DL, #ec_dvv{module=Mod, type=Type, option=Option}=DI, #ec_dvv{module=Mod, type=Type, option=Option}=State, ServerId) ->
    UpdateFun = Mod:update_fun_crdt([Type]),
    Delta = Mod:delta_crdt(Ops, DL, State, ServerId),
    case ec_dvv:is_valid(Delta) of
	true  ->
	    case ec_dvv:causal_consistent(Delta, State, 0, ServerId) of
		?EC_CAUSALLY_CONSISTENT ->
		    State1 = update(Delta, State, UpdateFun, ServerId),
		    DI1    = update(Delta, DI, UpdateFun, ServerId),
		    {ok, DI1, State1};
		?EC_CAUSALLY_AHEAD      ->
		    process_causally_ahead(State, ServerId);
		Reason                  ->
		    {error, Reason}
	    end;
	false  ->
	    {error, ?EC_INVALID_OPERATION}
    end.

-spec query(State :: #ec_dvv{}) -> term().
query(State) ->
    query(undefined, State).

-spec query(Criteria :: term(), State :: #ec_dvv{}) -> term().
query(Criteria, #ec_dvv{module=Mod}=State) ->
    Mod:query_crdt(Criteria, State).

-spec reset(DVV :: #ec_dvv{}) -> #ec_dvv{}.
reset(#ec_dvv{module=Mod}=DVV) ->
    Mod:reset_crdt(DVV).
			      
% private function

-spec update(Delta :: #ec_dvv{}, State :: #ec_dvv{}, UpdateFun :: {fun(), fun()}, ServerId :: term()) -> #ec_dvv{}.
update(#ec_dvv{module=Mod, type=Type, option=Option}=Delta, #ec_dvv{module=Mod, type=Type, option=Option}=State, UpdateFun, ServerId) ->
    State1 = ec_dvv:update(Delta, State, UpdateFun, ServerId),
    State2 = ec_crdt_util:add_param(State1, State),
    State3 = Mod:reconcile_crdt(State2, ServerId),
    ec_crdt_util:add_param(State3, State).

-spec process_causally_ahead(State :: #ec_dvv{}, ServerId :: term()) -> {error, {?EC_CAUSALLY_AHEAD, atom() | #ec_dot{}}}.
process_causally_ahead(#ec_dvv{}=State, ServerId) ->
    Msg = case ec_crdt_util:find_dot(State, ServerId) of
	      false         ->
		  ?EC_DOT_DOES_NOT_EXIST;
	      #ec_dot{}=Dot ->
		  Dot#ec_dot{values=[]}
	  end,
    {error, {?EC_CAUSALLY_AHEAD, Msg}}.


    

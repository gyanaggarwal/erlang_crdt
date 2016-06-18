%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%                                  
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

-module(ec_gen_ccrdt).

-behavior(ec_gen_crdt).

-export([new_crdt/2,
         delta_crdt/4,
         reconcile_crdt/3,
         update_fun_crdt/1,
         merge_fun_crdt/1,
         query_crdt/2,
         reset_crdt/2,
	 mutated_crdt/1,
	 causal_context_crdt/2,
         causal_consistent_crdt/5,
	 causal_history_crdt/3]).

-include("erlang_crdt.hrl").

-spec new_crdt(Type :: atom(), Name :: term()) -> #ec_dvv{}.
new_crdt(Type, Name) ->    
    #ec_dvv{module=?MODULE, type=Type, name=Name, annonymus_list=[new_annonymus_value()]}.

-spec delta_crdt(Ops :: term(), DL :: list(), State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.
delta_crdt({?EC_OPS_MUTATE, {{Type, Name}, Ops}}, DL, #ec_dvv{module=?MODULE, annonymus_list=[CMap]}=State, ServerId) ->
    #ec_dvv{module=Mod} = DVV = find_dvv({Type, Name}, CMap),
    Value = Mod:delta_crdt(Ops, DL, DVV, ServerId),
    ec_crdt_util:new_delta(Value, ec_dvv:join(State), State, ServerId).

-spec reconcile_crdt(State :: #ec_dvv{}, ServerId :: term(), Flag :: ?EC_LOCAL | ?EC_GLOBAL) -> #ec_dvv{}.
reconcile_crdt(#ec_dvv{module=?MODULE, dot_list=DL1, annonymus_list=[CMap1]}=State, ServerId, ?EC_LOCAL) ->
    #ec_dot{replica_id=ServerId, values=[#ec_dvv{type=Type1, name=Name1}=DVV1]} = Dot1 = ec_dvv:find_dot(DL1, ServerId),
    case ec_crdt_util:is_dirty(DVV1) of
	true  ->
	    DVV3 = find_dvv({Type1, Name1}, CMap1),
	    DVV4 = ec_gen_crdt:update(DVV1, DVV3, ServerId),
	    CMap2 = maps:put({Type1, Name1}, DVV4, CMap1),
	    DL2 = ec_dvv:replace_dot_list(DL1, ec_dvv:empty_dot(Dot1)),
	    State#ec_dvv{dot_list=DL2, annonymus_list=[CMap2]};
	false ->
	    State
    end;
reconcile_crdt(#ec_dvv{module=?MODULE, annonymus_list=AL}=State, ServerId, ?EC_GLOBAL) ->
    case AL of
	[CMap1, CMap2] ->
	    CMap3 = maps:fold(fun(K, V, Acc) -> merge_map_fun(K, V, Acc, ServerId) end, CMap2, CMap1),
	    State#ec_dvv{annonymus_list=[CMap3]};
	_              ->
	    State
    end.

-spec update_fun_crdt(Args :: list()) -> {fun(), fun()}.
update_fun_crdt([_Type]) ->     
    {fun ec_dvv:merge_default/3, fun ec_dvv:merge_default/3}.

-spec merge_fun_crdt(Args :: list()) -> {fun(), fun()}.
merge_fun_crdt([_Type]) ->
    {fun ec_dvv:merge_default/3, fun ec_dvv:merge_default/3}.

-spec reset_crdt(State :: #ec_dvv{}, ServerI :: term()) -> #ec_dvv{}.
reset_crdt(#ec_dvv{module=?MODULE, annonymus_list=[CMap]}=State, ServerId) ->
    CMap1 = maps:fold(fun(Key, DVV, Acc) -> maps:put(Key, ec_gen_crdt:reset(DVV, ServerId), Acc) end, maps:new(), CMap),
    ec_crdt_util:reset(State#ec_dvv{annonymus_list=[CMap1]}, ServerId, ?EC_RESET_NONE).

-spec mutated_crdt(DVV :: #ec_dvv{}) -> #ec_dvv{}.
mutated_crdt(#ec_dvv{module=?MODULE, annonymus_list=[CMap]}=DVV) ->
    CMap1 = maps:fold(fun mutated_crdt_fun/3, maps:new(), CMap),
    DVV#ec_dvv{annonymus_list=[CMap1]}.

-spec causal_consistent_crdt(Delta :: #ec_dvv{}, 
			     State :: #ec_dvv{}, 
			     ServerId :: term(),
			     Flag :: ?EC_LOCAL | ?EC_GLOBAL,
			     List :: list()) -> list().
causal_consistent_crdt(#ec_dvv{module=?MODULE, type=Type, name=Name, annonymus_list=[#ec_dvv{module=Mod, type=Type1, name=Name1}=DVV1]}=Delta,
		       #ec_dvv{module=?MODULE, type=Type, name=Name, annonymus_list=[CState]}=State,
		       ServerId,
		       ?EC_LOCAL,
		       List) ->
    L1 = ec_crdt_util:causal_consistent(Delta, State, ServerId, List),
    Mod:causal_consistent_crdt(DVV1, find_dvv({Type1, Name1}, CState), ServerId, ?EC_LOCAL, L1);
causal_consistent_crdt(#ec_dvv{module=?MODULE, type=Type, name=Name, annonymus_list=[CDelta]}=Delta,
		       #ec_dvv{module=?MODULE, type=Type, name=Name, annonymus_list=[CState]}=State,
		       ServerId,
		       ?EC_GLOBAL,
		       List) ->
    L1 = ec_crdt_util:causal_consistent(Delta, State, ServerId, List),
    maps:fold(fun(Key, #ec_dvv{module=Mod}=DVV, Acc) -> Mod:causal_consistent_crdt(DVV, find_dvv(Key, CState), ServerId, ?EC_GLOBAL, Acc) end, L1, CDelta).

-spec causal_history_crdt(State :: #ec_dvv{}, ServerId :: term(), Flag :: ?EC_CAUSAL_SERVER_ONLY | ?EC_CAUSAL_EXCLUDE_SERVER) -> #ec_dvv{}.
causal_history_crdt(#ec_dvv{module=?MODULE, annonymus_list=[CMap]}=State, ServerId, Flag) ->
    case ec_crdt_util:causal_history(State, ServerId, Flag) of
	?EC_UNDEFINED ->
	    ?EC_UNDEFINED;
	State1        ->
	    CMap1 = maps:fold(fun(KX, DVVX, Map) -> causal_history_crdt_fun(KX, DVVX, ServerId, Flag, Map) end, maps:new(), CMap),
	    State1#ec_dvv{annonymus_list=[CMap1]}
    end.

-spec query_crdt(Criteria :: term(), State :: #ec_dvv{}) -> {error, ?EC_INVALID_OPERATION} | term().
query_crdt([{Type, Name} | TCriteria], #ec_dvv{module=?MODULE, annonymus_list=[CMap]}) ->
    case maps:find({Type, Name}, CMap) of
	error                                   ->
	    {error, ?EC_INVALID_OPERATION};
	{ok, #ec_dvv{type=Type, name=Name}=DVV} ->
	    ec_gen_crdt:query(TCriteria, DVV)
    end.

-spec causal_context_crdt(Ops :: term(), State :: #ec_dvv{}) -> list().
causal_context_crdt({_, {{Type, Name}, Ops}}, #ec_dvv{module=?MODULE, annonymus_list=[CMap]}) ->
    case maps:find({Type, Name}, CMap) of
	error                                   ->
	    [];
	{ok, #ec_dvv{type=Type, name=Name}=DVV} ->
	    ec_gen_crdt:causal_context(Ops, DVV)
    end.

% private function

-spec new_annonymus_value() -> maps:map().
new_annonymus_value() ->
    maps:new().

-spec merge_map_fun({Type1 :: atom(), Name1 :: term()}, DVV1 :: #ec_dvv{}, CMap2 :: maps:map(), ServerId :: term()) -> maps:map().
merge_map_fun({Type1, Name1}, #ec_dvv{module=Mod, type=Type1, name=Name1}=DVV1, CMap2, ServerId) ->
    {ok, DVV4} = case maps:find({Type1, Name1}, CMap2) of
		     error ->
			 {ok, DVV1};
		     {ok, #ec_dvv{module=Mod, type=Type1, name=Name1}=DVV2} ->
			 case ec_gen_crdt:merge(DVV1, DVV2, ServerId) of
			     {error, _} ->
				 {ok, DVV1};
			     {ok, DVV3} ->
				 {ok, DVV3}
			 end
                 end,
    maps:put({Type1, Name1}, DVV4, CMap2).

-spec find_dvv({Type :: atom(), Name :: term()}, CMap :: maps:map()) -> #ec_dvv{} | ?EC_UNDEFINED.
find_dvv({Type, Name}, CMap) ->
    case maps:find({Type, Name}, CMap) of
	error     ->
            ec_gen_crdt:new(Type, Name);
	{ok, DVV} ->
	    DVV
    end.

-spec mutated_crdt_fun(K :: term(), DVV :: #ec_dvv{}, Map :: maps:map()) -> maps:map().
mutated_crdt_fun(K, DVV, Map) ->
    DVV1 = ec_gen_crdt:mutated(DVV),
    case ec_crdt_util:is_dirty(DVV1) of
	true  ->
	    maps:put(K, DVV1, Map);
	false ->
	    Map
    end.

-spec causal_history_crdt_fun(K :: term(), 
			      DVV :: #ec_dvv{}, 
			      ServerId :: term(), 
			      Flag :: ?EC_CAUSAL_SERVER_ONLY | ?EC_CAUSAL_EXCLUDE_SERVER, 
			      Map :: maps:map()) -> maps:map().
causal_history_crdt_fun(K, DVV, ServerId, Flag, Map) ->    
    case ec_gen_crdt:causal_history(DVV, ServerId, Flag) of
	?EC_UNDEFINED ->
	    Map;
	DVV1          ->
	    maps:put(K, DVV1, Map)
    end.




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

-module(ec_gen_scrdt).

-behavior(ec_gen_crdt).

-export([new_crdt/3,
	 delta_crdt/4,
	 reconcile_crdt/4,
	 update_fun_crdt/1,
	 merge_fun_crdt/1,
	 query_crdt/2,
	 reset_crdt/1,
	 mutated_crdt/1,
	 causal_list_crdt/2,
	 causal_consistent_crdt/5]).
	 
-include("erlang_crdt.hrl").

-spec new_crdt(Type :: atom(), Name :: term(), Args :: term()) -> #ec_dvv{}.
new_crdt(Type, Name, Args) ->
    #ec_dvv{module=?MODULE, type=Type, name=Name, option=Args, annonymus_list=[new_annonymus_value(Type)]}. 

-spec delta_crdt(Ops :: term(), DL :: list(), State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.
delta_crdt(Ops, DL, #ec_dvv{module=?MODULE}=State, ServerId) ->
    Value = {_, {_, CSet}} = new_value(Ops, State, ServerId),
    case sets:size(CSet) of
	0 ->
  	    ec_crdt_util:add_param(#ec_dvv{}, State);
	_ ->
	    ec_crdt_util:new_delta(Value, DL, State, ServerId)
    end.

-spec reconcile_crdt(State :: #ec_dvv{}, ServerId :: term(), Flag :: ?EC_LOCAL | ?EC_GLOBAL, DataStatus :: atom()) -> #ec_dvv{}.
reconcile_crdt(#ec_dvv{module=?MODULE, type=Type, dot_list=DL1, annonymus_list=[AD1]}=State, ServerId, ?EC_LOCAL, _DataStatus) ->
    Dot1 = ec_crdt_util:find_dot(State, ServerId),
    {Tag, V9} = case Dot1#ec_dot.values of
		    [V1]     ->
			{?EC_RECONCILE, V1};
		    [V1, V2] ->
			reconcile_local(V1, V2, Type)
		end,

    AD9 = case Tag of
	      ?EC_RECONCILED ->
		  AD1;
	      ?EC_RECONCILE  ->
		  reconcile(Type, ?EC_LOCAL, V9, AD1)
          end,

    Dot9 = Dot1#ec_dot{values=[V9]},
    DL9 = ec_dvv:replace_dot_list(DL1, Dot9),
    State#ec_dvv{dot_list=DL9, annonymus_list=[AD9]};
reconcile_crdt(#ec_dvv{module=?MODULE, type=Type, annonymus_list=[D1, D2]}=State, _ServerId, ?EC_GLOBAL, _DataStatus) ->			     
    D3 = reconcile(Type, ?EC_GLOBAL, D1, D2),
    State#ec_dvv{annonymus_list=[D3]};
reconcile_crdt(#ec_dvv{module=?MODULE}=State, _ServerId, ?EC_GLOBAL, _DataStatus) ->
    State.

-spec update_fun_crdt(Args :: list()) -> {fun(), fun()}.
update_fun_crdt([_Type]) -> 
    {fun ec_dvv:merge_default/3, fun ec_dvv:merge_default/3}.

-spec merge_fun_crdt(Args :: list()) -> {fun(), fun()}.
merge_fun_crdt([_Type]) ->
    {fun ec_dvv:merge_default/3, fun ec_dvv:merge_default/3}.

-spec reset_crdt(State :: #ec_dvv{}) -> #ec_dvv{}.
reset_crdt(#ec_dvv{module=?MODULE, type=Type}=State) ->
    State1 = ec_crdt_util:reset(State, ?EC_RESET_ANNONYMUS_LIST),
    State1#ec_dvv{annonymus_list=[new_annonymus_value(Type)]}.

-spec mutated_crdt(DVV :: #ec_dvv{}) -> #ec_dvv{}.
mutated_crdt(DVV) ->
    DVV.

-spec causal_list_crdt(Ops :: term(), State :: #ec_dvv{}) -> list().
causal_list_crdt(_Ops, #ec_dvv{module=?MODULE}=State) ->
    ec_dvv:join(State).

-spec causal_consistent_crdt(Delta :: #ec_dvv{}, 
			     State :: #ec_dvv{}, 
			     Offset :: non_neg_integer(), 
			     ServerId :: term(),
			     Flag :: ?EC_LOCAL | ?EC_GLOBAL) -> ?EC_CAUSALLY_CONSISTENT | list().
causal_consistent_crdt(#ec_dvv{module=?MODULE, type=Type, name=Name, option=Option}=Delta, 
		       #ec_dvv{module=?MODULE, type=Type, name=Name, option=Option}=State,
		       Offset,
		       ServerId,
		       _Flag) ->
    case ec_dvv:causal_consistent(Delta, State, Offset, ServerId) of
	?EC_CAUSALLY_CONSISTENT ->
	    ?EC_CAUSALLY_CONSISTENT;
	Reason                  ->
	    [Reason]
    end.

-spec query_crdt(Criteria :: term(), State :: #ec_dvv{}) -> sets:set() | maps:map() | {ok, term()} | error.
query_crdt([Criteria], #ec_dvv{module=?MODULE}=State)                                            ->
    query_crdt(Criteria, State);
query_crdt([], #ec_dvv{module=?MODULE}=State)                                                    ->
    query_crdt(?EC_UNDEFINED, State);
query_crdt(?EC_UNDEFINED, #ec_dvv{module=?MODULE, type=?EC_AWORSET, annonymus_list=[{VSet, _}]}) ->
    ec_sets_util:get_value_set(VSet);
query_crdt(?EC_UNDEFINED, #ec_dvv{module=?MODULE, type=?EC_RWORSET, annonymus_list=[{VSet, _}]}) ->
    ec_sets_util:get_value_set(VSet);
query_crdt(?EC_UNDEFINED, #ec_dvv{module=?MODULE, type=?EC_PWORMAP, annonymus_list=[{VMap, _}]}) ->
    ec_sets_util:get_value_map(VMap);
query_crdt(?EC_UNDEFINED, #ec_dvv{module=?MODULE, type=?EC_RWORMAP, annonymus_list=[{VMap, _}]}) ->
    ec_sets_util:get_value_map(VMap);
query_crdt(Criteria,  #ec_dvv{module=?MODULE, type=?EC_PWORMAP, annonymus_list=[{VMap, _}]})     ->
    maps:find(Criteria, ec_sets_util:get_value_map(VMap));
query_crdt(Criteria,  #ec_dvv{module=?MODULE, type=?EC_RWORMAP, annonymus_list=[{VMap, _}]})     ->
    maps:find(Criteria, ec_sets_util:get_value_map(VMap)).

% private function

-spec reconcile_local({Ops1 :: term(), {V1 :: sets:set() | maps:map(), C1 :: sets:set()}},
		      {Ops2 :: term(), {V2 :: sets:set() | maps:map(), C2 :: sets:set()}},
		      Type :: ?EC_AWORSET | ?EC_RWORSET | ?EC_PWORMAP | ?EC_RWORMAP) 
		     -> {?EC_RECONCILE | ?EC_RECONCILED, {term(), {sets:set() | maps:map(), sets:set()}}}.
reconcile_local({{?EC_OPS_RMV, Value}, _}, {{?EC_OPS_ADD, Value}, _}=D2, ?EC_AWORSET)                         ->
    {?EC_RECONCILED, D2};
reconcile_local({{?EC_OPS_ADD, Value}, _}, {{?EC_OPS_RMV, Value}, _}=D2, ?EC_RWORSET)                         ->
    {?EC_RECONCILED, D2};
reconcile_local({{?EC_OPS_RMV, Key}, _}, {{?EC_OPS_PUT, {Key, _}}, _}=D2, ?EC_PWORMAP)                        ->
    {?EC_RECONCILED, D2};
reconcile_local({{?EC_OPS_PUT, {Key, _}}, _}, {{?EC_OPS_RMV, Key}, _}=D2, ?EC_RWORMAP)                        ->
    {?EC_RECONCILED, D2};
reconcile_local({{?EC_OPS_PUT, {Key, Value1}}=Ops, {V1, C1}}, {{?EC_OPS_PUT, {Key, Value2}}, _}, ?EC_PWORMAP) ->
    {?EC_RECONCILE, {Ops, {ec_sets_util:update_latest_entry(V1, Value1, Value2), C1}}};
reconcile_local({{?EC_OPS_PUT, {Key, Value1}}=Ops, {V1, C1}}, {{?EC_OPS_PUT, {Key, Value2}}, _}, ?EC_RWORMAP) ->
    {?EC_RECONCILE, {Ops, {ec_sets_util:update_latest_entry(V1, Value1, Value2), C1}}};
reconcile_local(D1, _D2, _Type)                                                                               ->
    {?EC_RECONCILE, D1}.

-spec new_value(Ops :: term(), State :: #ec_dvv{}, ServerId :: term()) -> {term(), {sets:set() | maps:map(), sets:set()}}.
new_value({?EC_OPS_ADD, Value}=Ops, #ec_dvv{module=?MODULE, type=?EC_AWORSET}=State, ServerId)                      ->
    {Ops, add_value_set(Value, State, ServerId)};
new_value({?EC_OPS_ADD, Value}=Ops, #ec_dvv{module=?MODULE, type=?EC_RWORSET}=State, ServerId)                      ->
    {Ops, add_value_set(Value, State, ServerId)};
new_value({?EC_OPS_RMV, Value}=Ops, #ec_dvv{module=?MODULE, type=?EC_AWORSET, annonymus_list=[{V1, _}]}, _ServerId) ->
    {Ops, rmv_value_set(Value, V1)};
new_value({?EC_OPS_RMV, Value}=Ops, #ec_dvv{module=?MODULE, type=?EC_RWORSET, annonymus_list=[{V1, _}]}, _ServerId) ->    
    {Ops, rmv_value_set(Value, V1)};
new_value({?EC_OPS_PUT, {Key, Value}}=Ops, #ec_dvv{module=?MODULE, type=?EC_PWORMAP}=State, ServerId)               ->
    {Ops, add_value_map(Key, Value, State, ServerId)};
new_value({?EC_OPS_PUT, {Key, Value}}=Ops, #ec_dvv{module=?MODULE, type=?EC_RWORMAP}=State, ServerId)               ->
    {Ops, add_value_map(Key, Value, State, ServerId)};
new_value({?EC_OPS_RMV, Key}=Ops, #ec_dvv{module=?MODULE, type=?EC_PWORMAP, annonymus_list=[{V1, _}]}, _ServerId)   ->
    {Ops, rmv_value_map(Key, V1)};
new_value({?EC_OPS_RMV, Key}=Ops, #ec_dvv{module=?MODULE, type=?EC_RWORMAP, annonymus_list=[{V1, _}]}, _ServerId)   ->
    {Ops, rmv_value_map(Key, V1)}.

-spec new_annonymus_value(Type :: ?EC_AWORSET | ?EC_RWORSET | ?EC_PWORMAP | ?EC_RWORMAP) 
			 -> {maps:map() | sets:set(), sets:set()}.
new_annonymus_value(Type) ->
    case Type of
	?EC_AWORSET ->
	    {sets:new(), sets:new()};
	?EC_RWORSET ->
	    {sets:new(), sets:new()};
	?EC_PWORMAP ->
	    {maps:new(), sets:new()};
	?EC_RWORMAP ->
	    {maps:new(), sets:new()}
    end.

-spec reconcile(Type :: ?EC_AWORSET | ?EC_RWORSET | ?EC_PWORMAP | ?EC_RWORMAP,
		Flag :: ?EC_LOCAL | ?EC_GLOBAL,
		V1 :: term(),
		V2 :: term()) -> {maps:map() | sets:set(), sets:set()}.
reconcile(?EC_AWORSET, ?EC_LOCAL, {_, {V1, C1}}, {V2, C2}) ->
    ec_sets_util:merge_set({V1, C1}, {V2, C2});
reconcile(?EC_RWORSET, ?EC_LOCAL, {_, {V1, C1}}, {V2, C2}) ->
    ec_sets_util:merge_set({V1, C1}, {V2, C2});
reconcile(?EC_AWORSET, ?EC_GLOBAL, {V1, C1}, {V2, C2})     ->
    ec_sets_util:merge_set({V1, C1}, {V2, C2});
reconcile(?EC_RWORSET, ?EC_GLOBAL, {V1, C1}, {V2, C2})     ->
    ec_sets_util:merge_set({V1, C1}, {V2, C2});
reconcile(?EC_PWORMAP, ?EC_LOCAL, {_, {V1, C1}}, {V2, C2}) ->
    ec_sets_util:merge_map({V1, C1}, {V2, C2});
reconcile(?EC_RWORMAP, ?EC_LOCAL, {_, {V1, C1}}, {V2, C2}) ->
    ec_sets_util:merge_map({V1, C1}, {V2, C2});
reconcile(?EC_PWORMAP, ?EC_GLOBAL, {V1, C1}, {V2, C2}) ->
    ec_sets_util:merge_map({V1, C1}, {V2, C2});
reconcile(?EC_RWORMAP, ?EC_GLOBAL, {V1, C1}, {V2, C2}) ->
    ec_sets_util:merge_map({V1, C1}, {V2, C2}).

-spec next_counter_value(State :: #ec_dvv{}, ServerId :: term()) -> non_neg_integer().
next_counter_value(#ec_dvv{module=?MODULE}=State, ServerId) ->    
    case ec_crdt_util:find_dot(State, ServerId) of
        false                    ->
            1;
	#ec_dot{counter_max=Max} ->            
	    Max+1
    end.

-spec add_value(Value :: term(), State :: #ec_dvv{}, ServerId :: term()) -> {{term(), non_neg_integer(), term()}, sets:set()}.
add_value(Value, #ec_dvv{module=?MODULE}=State, ServerId) ->
    C1 = next_counter_value(State, ServerId),
    CSet = sets:from_list([{ServerId, C1}]),
    {{ServerId, C1, Value}, CSet}.

-spec add_value_set(Value :: term(), State :: #ec_dvv{}, ServerId :: term()) -> {sets:set(), sets:set()}.
add_value_set(Value, State, ServerId) ->
    {S, CSet} = add_value(Value, State, ServerId),
    {sets:from_list([S]), CSet}.

-spec add_value_map(Key :: term(), Value :: term(), State :: #ec_dvv{}, ServerId :: term()) -> {maps:map(), sets:set()}.
add_value_map(Key, Value, State, ServerId) ->
    {S, CSet} = add_value(Key, State, ServerId),
    {maps:from_list([{S, sets:from_list([Value])}]), CSet}.

-spec rmv_value_set(Value :: term(), VSet :: sets:set()) -> {sets:set(), sets:set()}.
rmv_value_set(Value, VSet) ->
   {sets:new(), ec_sets_util:causal_from_set(Value, VSet)}.

-spec rmv_value_map(Key :: term(), VMap :: maps:map()) -> {maps:map(), sets:set()}.
rmv_value_map(Key, VMap) ->    
   {maps:new(), ec_sets_util:causal_from_map(Key, VMap)}.

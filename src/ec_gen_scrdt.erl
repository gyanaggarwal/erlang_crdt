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

-export([new_crdt/2,
	 delta_crdt/4,
	 reconcile_crdt/3,
	 update_fun_crdt/1,
	 merge_fun_crdt/1,
	 query_crdt/2,
	 reset_crdt/1,
	 causal_consistent_crdt/4]).
	 
-include("erlang_crdt.hrl").

-spec new_crdt(Type :: atom(), Args :: term()) -> #ec_dvv{}.
new_crdt(Type, Args) ->
    #ec_dvv{module=?MODULE, type=Type, option=Args, annonymus_list=[new_annonymus_value()]}. 

-spec delta_crdt(Ops :: term(), DL :: list(), State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.
delta_crdt(Ops, DL, #ec_dvv{module=?MODULE}=State, ServerId) ->
    case new_value(Ops, State, ServerId) of
	{_, {undefined, undefined, undefined}} ->
	    ec_crdt_util:add_param(#ec_dvv{}, State);
	Value                                  ->
	    ec_crdt_util:new_delta(Value, DL, State, ServerId)
    end.

-spec reconcile_crdt(State :: #ec_dvv{}, ServerId :: term(), Flag :: ?EC_RECONCILE_LOCAL | ?EC_RECONCILE_GLOBAL) -> #ec_dvv{}.
reconcile_crdt(#ec_dvv{module=?MODULE, type=Type, dot_list=DL1, annonymus_list=[AD1]}=State, ServerId, ?EC_RECONCILE_LOCAL) ->
    Dot1 = ec_crdt_util:find_dot(State, ServerId),
    {Tag, V9} = case Dot1#ec_dot.values of
		    [V1]     ->
			{?EC_RECONCILE, V1};
		    [V1, V2] ->
			reconcile_local(V1, V2, Type)
		end,
    AD9 = case {Tag, Type} of
	      {?EC_RECONCILED, _} ->
		  AD1;
	      {_, ?EC_AWORSET}   ->
		  add_win(V9, AD1, ?EC_RECONCILE_LOCAL);
	      {_, ?EC_RWORSET}   ->
		  rmv_win(V9, AD1, ?EC_RECONCILE_LOCAL)
	  end,
    Dot9 = Dot1#ec_dot{values=[V9]},
    DL9 = ec_dvv:replace_dot_list(DL1, Dot9),
    State#ec_dvv{dot_list=DL9, annonymus_list=[AD9]};
reconcile_crdt(#ec_dvv{module=?MODULE, type=Type, annonymus_list=[D1, D2]}=State, _ServerId, ?EC_RECONCILE_GLOBAL) ->			     
    D3 = case Type of
	     ?EC_AWORSET ->
		 add_win(D1, D2, ?EC_RECONCILE_GLOBAL);
	     ?EC_RWORSET ->
		 rmv_win(D1, D2, ?EC_RECONCILE_GLOBAL)
	 end,
    State#ec_dvv{annonymus_list=[D3]};
reconcile_crdt(State, _ServerId, ?EC_RECONCILE_GLOBAL) ->
    State.

-spec update_fun_crdt(Args :: list()) -> {fun(), fun()}.
update_fun_crdt([_Type]) -> 
    {fun ec_dvv:merge_default/3, fun ec_dvv:merge_default/3}.

-spec merge_fun_crdt(Args :: list()) -> {fun(), fun()}.
merge_fun_crdt([_Type]) ->
    {fun ec_dvv:merge_default/3, fun ec_dvv:merge_default/3}.

-spec reset_crdt(State :: #ec_dvv{}) -> #ec_dvv{}.
reset_crdt(#ec_dvv{module=?MODULE}=State) ->
    State1 = ec_crdt_util:reset(State, ?EC_RESET_ANNONYMUS_LIST),
    State1#ec_dvv{annonymus_list=[new_annonymus_value()]}.

-spec causal_consistent_crdt(Delta :: #ec_dvv{}, State :: #ec_dvv{}, Offset :: non_neg_integer(), ServerId :: term()) -> ?EC_CAUSALLY_CONSISTENT | 
															 ?EC_CAUSALLY_AHEAD |
															 ?EC_CAUSALLY_BEHIND.
causal_consistent_crdt(#ec_dvv{module=?MODULE, type=Type, option=Option}=Delta, 
		       #ec_dvv{module=?MODULE, type=Type, option=Option}=State,
		       Offset,
		       ServerId) ->
    ec_dvv:causal_consistent(Delta, State, Offset, ServerId).

-spec query_crdt(Criteria :: term(), State :: #ec_dvv{}) -> term().
query_crdt(_Criteria, #ec_dvv{module=?MODULE, annonymus_list=[{VSet, _RSet, _CSet}]}) ->
    get_elements(VSet).

% private function

-spec reconcile_local({?EC_OPS_ADD | ?EC_OPS_RMV, {VSet1 :: sets:set(), RSet1 :: sets:set(), CSet1 :: sets:set()}},
		      {?EC_OPS_ADD | ?EC_OPS_RMV, {VSet2 :: sets:set(), RSet2 :: sets:set(), CSet2 :: sets:set()}},
		      Type :: ?EC_AWORSET | ?EC_RWORSET) 
		     -> {?EC_RECONCILE | ?EC_RECONCILED, {?EC_OPS_ADD | ?EC_OPS_RMV, {sets:set(), sets:set(), sets:set()}}}.
reconcile_local({?EC_OPS_RMV, {_V1, _R1, C1}}=D1, {?EC_OPS_ADD, {_V2, _R2, C2}}=D2, ?EC_AWORSET) ->
    CI = sets:intersection(C1, C2),
    case sets:size(CI) of 
	0 -> 
	    {?EC_RECONCILE, D1};
	_ ->
	    {?EC_RECONCILED, D2}
    end;
reconcile_local({?EC_OPS_ADD, {V1, _R1, _C1}}=D1, {?EC_OPS_RMV, {_V2, R2, _C2}}=D2, ?EC_RWORSET) ->
    E1 = get_elements(V1),
    RI = sets:intersection(E1, R2),
    case sets:size(RI) of
	0 ->
	    {?EC_RECONCILE, D1};
	_ ->
	    {?EC_RECONCILED, D2}
    end;
reconcile_local(D1, _D2, _Type) ->
    {?EC_RECONCILE, D1}.
			     
-spec add_win(D1 :: term(), D2 :: term(), Flag :: ?EC_RECONCILE_LOCAL | ?EC_RECONCILE_GLOBAL) -> term().
add_win({_, {VSet1, RSet1, CSet1}},
	{VSet2, RSet2, CSet2},
	?EC_RECONCILE_LOCAL) ->
    add_logic({VSet1, RSet1, CSet1}, {VSet2, RSet2, CSet2});
add_win({VSet1, RSet1, CSet1},
        {VSet2, RSet2, CSet2},
        ?EC_RECONCILE_GLOBAL) ->
    add_logic({VSet1, RSet1, CSet1}, {VSet2, RSet2, CSet2}).

-spec rmv_win(D1 :: term(), D2 :: term(), Flag :: ?EC_RECONCILE_LOCAL | ?EC_RECONCILE_GLOBAL) -> term().
rmv_win({?EC_OPS_ADD, {VSet1, RSet1, CSet1}},
	{VSet2, RSet2, CSet2},
	?EC_RECONCILE_LOCAL) ->
    {VSet, RSet, CSet} = add_logic({VSet1, RSet1, CSet1}, {VSet2, RSet2, CSet2}),
    {VSet, sets:subtract(RSet, get_elements(VSet1)), CSet};
rmv_win({?EC_OPS_RMV, {VSet1, RSet1, CSet1}},
	{VSet2, RSet2, CSet2},
	?EC_RECONCILE_LOCAL) ->
    rmv_logic({VSet1, RSet1, CSet1}, {VSet2, RSet2, CSet2});
rmv_win({VSet1, RSet1, CSet1},
	{VSet2, RSet2, CSet2},
	?EC_RECONCILE_GLOBAL) ->
    rmv_logic({VSet1, RSet1, CSet1}, {VSet2, RSet2, CSet2}).

-spec add_logic(D1 :: term(), D2 :: term()) -> term().
add_logic({VSet1, RSet1, CSet1}, {VSet2, RSet2, CSet2}) ->
    CSet = sets:union(CSet1, CSet2),
    RSet = sets:union(RSet1, RSet2),
    VSet = sets:union(sets:intersection(VSet1, VSet2),
                      sets:union(get_add_element_set(VSet1, CSet2),
                                 get_add_element_set(VSet2, CSet1))),
    {VSet, RSet, CSet}. 
    
-spec rmv_logic(D1 :: term(), D2 :: term()) -> term().
rmv_logic({VSet1, RSet1, CSet1}, {VSet2, RSet2, CSet2}) ->
    CSet = sets:union(CSet1, CSet2),
    RSet = sets:union(RSet1, RSet2),
    VSet = sets:subtract(sets:union(VSet1, VSet2), get_cartesian_product(RSet, CSet)),
    {VSet, RSet, CSet}.

-spec next_counter_value(State :: #ec_dvv{}, ServerId :: term()) -> non_neg_integer().
next_counter_value(#ec_dvv{module=?MODULE}=State, ServerId) ->
    case ec_crdt_util:find_dot(State, ServerId) of
	false                    ->
	    1;
	#ec_dot{counter_max=Max} ->
	    Max+1
    end.

-spec new_value(Ops :: term(), State :: #ec_dvv{}, ServerId :: term()) -> {sets:set() | undefined, sets:set() | undefined, sets:set() | undefined}.
new_value({?EC_OPS_ADD, Value}, #ec_dvv{module=?MODULE}=State, ServerId) ->
    C1 = next_counter_value(State, ServerId),
    VSet = sets:add_element({ServerId, C1, Value}, sets:new()),
    CSet = sets:add_element({ServerId, C1}, sets:new()),
    {?EC_OPS_ADD, {VSet, sets:new(), CSet}};
new_value({?EC_OPS_RMV, Value}, #ec_dvv{module=?MODULE, type=Type, annonymus_list=[{VSet, _RSet, _CSet}]}, _ServerId) ->
    CSet = sets:fold(fun(X, Set) -> get_element_set(Value, X, Set) end, sets:new(), VSet),
    D1 = case {sets:size(CSet) > 0, Type} of
	     {true, ?EC_AWORSET} ->
		 {sets:new(), sets:new(), CSet};
	     {true, ?EC_RWORSET} ->
		 {sets:new(), sets:from_list([Value]), CSet};
	     {false, _} ->
		 {undefined, undefined, undefined}
	 end,
    {?EC_OPS_RMV, D1}.

-spec new_annonymus_value() -> {sets:set(), sets:set(), sets:set()}.
new_annonymus_value() ->
    {sets:new(), sets:new(), sets:new()}.

-spec get_cartesian_product(Set1 :: sets:set(), Set2 :: sets:set()) -> sets:set().
get_cartesian_product(RSet, CSet) ->
    sets:fold(fun(X, SetX) -> get_value_set(X, CSet, SetX) end, sets:new(), RSet).

-spec get_value_set(R :: term(), CSet :: sets:set(), Set :: sets:set()) -> sets:set().
get_value_set(R, CSet, Set) ->
    sets:fold(fun({S, N}, SetX) -> sets:add_element({S, N, R}, SetX) end, Set, CSet).
		      
-spec get_elements(ElementSet :: sets:set()) -> sets:set().
get_elements(ElementSet) ->
    sets:fold(fun get_elements/2, sets:new(), ElementSet).
    
-spec get_elements(Element :: term(), Set :: sets:set()) -> sets:set().
get_elements({_, _, Element}, Set) ->
    sets:add_element(Element, Set).

-spec get_element_set(Element1 :: term(), {ServerId :: term(), Counter :: non_neg_integer(), Element :: term()}, Set :: sets:set()) -> sets:set().
get_element_set(Element, {ServerId, Counter, Element}, Set) ->
    sets:add_element({ServerId, Counter}, Set);
get_element_set(_Element, _SetElement, Set) ->
    Set.

-spec get_element(Element :: term(), Flag :: true | false, RefSet :: sets:set(), Set :: sets:set()) -> sets:set().
get_element({ServerId, Counter, _}=Element, Flag, RefSet, Set) ->
    case sets:is_element({ServerId, Counter}, RefSet) =:= Flag of
	false ->
	    Set;
	true  ->
	    sets:add_element(Element, Set)
    end.

-spec get_add_element_set(ElementSet :: sets:set(), RefElementSet :: sets:set()) -> sets:set().
get_add_element_set(ElementSet, RefElementSet) ->
    sets:fold(fun(X, SetX) -> get_element(X, false, RefElementSet, SetX) end, sets:new(), ElementSet).




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

-module(ec_sets_util).

-export([merge_set/2,
	 causal_from_set/2,
	 get_value_set/1,
	 merge_map/2,
	 causal_from_map/2,
	 get_value_map/1,
	 update_latest_entry/3,
	 pretty/1]).

-spec merge_set(D1 :: {sets:set(), sets:set()}, D2 :: {sets:set(), sets:set()}) -> {sets:set(), sets:set()}.
merge_set({VSet1, CSet1}, {VSet2, CSet2}) ->
    merge({VSet1, CSet1},
	  {VSet2, CSet2},
	  fun sets:union/2,
	  fun sets:intersection/2,
	  fun find_value_set/2).

-spec merge_map(D1 :: {maps:map(), sets:set()}, D2 :: {maps:map(), sets:set()}) -> {maps:map(), sets:set()}.
merge_map({VMap1, CSet1}, {VMap2, CSet2}) ->
    merge({VMap1, CSet1}, 
	  {VMap2, CSet2},
	  fun maps:merge/2,
	  fun map_intersection/2,
	  fun find_value_map/2).

-spec causal_from_set(Value :: term(), VSet :: sets:set()) -> sets:set().
causal_from_set(Value, VSet) ->
    sets:fold(fun(E, Acc) -> add_causal_set(Value, E, Acc) end, sets:new(), VSet).

-spec causal_from_map(Key :: term(), VMap :: maps:map()) -> sets:sets().
causal_from_map(Key, VMap) ->
    maps:fold(fun(K, _V, Acc) -> add_causal_set(Key, K, Acc) end, sets:new(), VMap).
		      
-spec get_value_set(VSet :: sets:set()) -> sets:set().
get_value_set(VSet) ->
    sets:fold(fun({_, _, Value}, Acc) -> sets:add_element(Value, Acc) end, sets:new(), VSet).
		      
-spec get_value_map(VMap :: maps:map()) -> maps:map().
get_value_map(VMap) ->
    M1 = maps:fold(fun({ServerId, Index, Key}, Value, Acc) -> put_inner_map(Key, ServerId, Index, Value, Acc) end, maps:new(), VMap),
    maps:fold(fun put_outer_map/3, maps:new(), M1).
			   
-spec update_latest_entry(Map :: maps:map(), Value1 :: term(), Value2 :: term()) -> maps:map().
update_latest_entry(Map, Value1, Value2) ->
    [Key] = maps:keys(Map),
    maps:put(Key, sets:from_list([Value1, Value2]), Map).

-spec pretty(V :: term()) -> term().
pretty(V) ->
    case is_map(V) of
	true  ->
	    maps:fold(fun(KX, VX, Acc) -> [{KX, pretty(VX)} | Acc] end, [], V);
	false ->
		case sets:is_set(V) of
		    true  ->
			sets:fold(fun(E, Acc) -> [pretty(E) | Acc] end, [], V);
		    false ->
			V
	        end
     end.
	    
% private function

-spec put_inner_map(Key :: term(), ServerId :: term(), Index :: non_neg_integer(), Value :: term(), IMap :: maps:map()) -> maps:map().
put_inner_map(Key, ServerId, Index, Value, IMap) ->
    ValueMap1 = case maps:find(Key, IMap) of
		    error          ->
			maps:new();
		    {ok, ValueMap} ->
			ValueMap
	        end,
    ValueMap9 = case maps:find(ServerId, ValueMap1) of
		    error                                 ->
			maps:put(ServerId, {Index, Value}, ValueMap1);
		    {ok, {Index1, _}} when Index1 < Index ->
			maps:put(ServerId, {Index, Value}, ValueMap1);
		    {ok, _}                               ->
			ValueMap1
                end, 
    maps:put(Key, ValueMap9, IMap).

-spec put_outer_map(Key :: term(), IMap :: maps:map(), OMap :: maps:map()) -> maps:map().
put_outer_map(Key, IMap, OMap) ->
    Set = maps:fold(fun(_, {_, Value}, AccSet) -> sets:union(Value, AccSet) end, sets:new(), IMap),
    maps:put(Key, Set, OMap).    

-spec value_set(E :: term(), RefSet :: sets:set(), AccSet :: sets:set()) -> sets:set().
value_set({ServerId, Counter, _}=E,  RefSet, AccSet) ->    
    case sets:is_element({ServerId, Counter}, RefSet) of
        true  ->
            AccSet;
        false -> 
            sets:add_element(E, AccSet)
    end.

-spec value_map(K :: term(), V :: term(), RefSet :: sets:set(), AccMap :: maps:map()) -> maps:map().
value_map({ServerId, Counter, _}=K, V, RefSet, AccMap) ->
    case sets:is_element({ServerId, Counter}, RefSet) of
	true  ->
	    AccMap;
	false ->
	    maps:put(K, V, AccMap)
    end.

-spec find_value_set(VSet1 :: sets:set(), CSet2 :: sets:set()) -> sets:set().
find_value_set(VSet1, CSet2) ->
    sets:fold(fun(E, AccSet) -> value_set(E, CSet2, AccSet) end, sets:new(), VSet1).

-spec find_value_map(VMap1 :: maps:map(), CSet2 :: sets:set()) -> maps:map().
find_value_map(VMap1, CSet2) ->
    maps:fold(fun(K, V, AccMap) -> value_map(K, V, CSet2, AccMap) end, maps:new(), VMap1).
		      
-spec add_map(K :: term(), V :: term(), RefMap :: maps:map(), AccMap :: maps:map()) -> maps:map().
add_map(K, V, RefMap, AccMap) -> 
    case maps:is_key(K, RefMap) of
	true  ->
	    maps:put(K, V, AccMap);
	false ->
	    AccMap
    end.

-spec map_intersection(VMap1 :: maps:map(), VMap2 :: maps:map()) -> maps:map().
map_intersection(VMap1, VMap2) ->
    maps:fold(fun(K, V, AccMap) -> add_map(K, V, VMap1, AccMap) end, maps:new(), VMap2).
		      
-spec add_causal_set(E1 :: term(), {ServerId :: term(), Counter :: non_neg_integer(), E2 :: term()}, Set :: sets:set()) -> sets:set().
add_causal_set(E, {ServerId, Counter, E}, Set) ->    
    sets:add_element({ServerId, Counter}, Set);
add_causal_set(_, _, Set) ->
    Set.

-spec merge(D1 :: {sets:set() | maps:map(), sets:set()}, 
	    D2 :: {sets:set() | maps:map(), sets:set()},
	    UFun :: fun(),
	    IFun :: fun(),
	    FFun :: fun()) -> {sets:set() | maps:map(), sets:set()}.
merge({V1, CSet1}, {V2, CSet2}, UFun, IFun, FFun) ->     
    CSet = sets:union(CSet1, CSet2),
    V    = UFun(IFun(V1, V2),
                UFun(FFun(V1, CSet2),
                     FFun(V2, CSet1))),
    {V, CSet}.





			


    


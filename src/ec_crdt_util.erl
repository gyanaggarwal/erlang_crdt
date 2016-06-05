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

-module(ec_crdt_util).

-export([add_param/2, 
	 new_delta/4,
	 find_dot/2,
	 find_module/1,
	 is_dirty/1,
	 is_state/1,
	 is_valid/1,
	 delta_state_pair/1,
	 reset/2]).

-include("erlang_crdt.hrl").

-spec add_param(DVV :: #ec_dvv{}, State :: #ec_dvv{}) -> #ec_dvv{}.
add_param(DVV, #ec_dvv{module=Mod, type=Type, name=Name, option=Option}) ->
    DVV#ec_dvv{module=Mod, type=Type, name=Name, option=Option}.

-spec new_delta(Value :: term(), DL :: list(), State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.
new_delta(Value, DL, State, ServerId) ->
    NewDL = case find_dot(DL, ServerId) of
                false ->        
                    [];
                Dot   ->
                    [Dot]
            end,
    add_param(ec_dvv:new(NewDL, Value), State).

-spec find_dot(DX :: list() | #ec_dvv{}, ServerId :: term()) -> false | #ec_dot{}.
find_dot(DL, ServerId) when is_list(DL) ->
    lists:keyfind(ServerId, #ec_dot.replica_id, DL);
find_dot(#ec_dvv{dot_list=DL}, ServerId) ->
    find_dot(DL, ServerId).

-spec reset(DVV :: #ec_dvv{}, Flag :: ?EC_RESET_NONE | ?EC_RESET_VALUES | ?EC_RESET_ANNONYMUS_LIST | ?EC_RESET_ALL | ?EC_RESET_VALUES_ONLY) -> #ec_dvv{}.
reset(#ec_dvv{}=DVV, ?EC_RESET_RETAIN_ALL) ->
    DVV;
reset(#ec_dvv{dot_list=DL}=DVV, ?EC_RESET_ANNONYMUS_LIST) ->
    DVV#ec_dvv{dot_list=reset_dot_list(DL, ?EC_RESET_NONE), annonymus_list=[]};
reset(#ec_dvv{dot_list=DL}=DVV, ?EC_RESET_ALL) ->
    DVV#ec_dvv{dot_list=reset_dot_list(DL, ?EC_RESET_VALUES), annonymus_list=[]};
reset(#ec_dvv{dot_list=DL}=DVV, Flag) ->
    DVV#ec_dvv{dot_list=reset_dot_list(DL, Flag)}.

-spec find_module(Type :: atom()) -> atom() | error.
find_module(Type) ->
    case maps:find(Type, ?EC_CRDT_MAP) of
	error     ->
	    error;
	{ok, Mod} ->
	    Mod
    end.

-spec is_valid(DVV :: #ec_dvv{} | ?EC_UNDEFINED) -> true | false.
is_valid(?EC_UNDEFINED) ->    
    false;
is_valid(#ec_dvv{dot_list=DL, annonymus_list=AL}) ->
    lists:foldl(fun(#ec_dot{values=VS}, Flag) -> Flag orelse length(VS) > 0 end, length(AL) > 0, DL).

-spec is_dirty(DVV :: #ec_dvv{}) -> true | false.
is_dirty(#ec_dvv{status=Status}) ->
    Status =:= ?EC_DVV_DIRTY_STATE orelse Status =:= ?EC_DVV_DIRTY_DELTA.

-spec is_state(DVV :: #ec_dvv{}) -> true | false.
is_state(#ec_dvv{status=Status}) ->
    Status =:= ?EC_DVV_DIRTY_STATE orelse Status =:= ?EC_DVV_CLEAN_STATE.

-spec delta_state_pair({Delta :: #ec_dvv{}, State :: #ec_dvv{}}) -> {#ec_dvv{}, #ec_dvv{}}.
delta_state_pair({Delta, State}) ->
    case is_state(State) of
	true  ->
	    {Delta, State};
	false ->
	    {State, Delta}
    end.

% private function

-spec reset_dot_list(DL :: list(), Flag :: ?EC_RESET_NONE | ?EC_RESET_VALUES | ?EC_RESET_VALUES_ONLY) -> list().
reset_dot_list(DL, ?EC_RESET_NONE) ->
    lists:foldl(fun(#ec_dot{counter_max=Max}=DotX, Acc) -> [DotX#ec_dot{counter_min=Max} | Acc] end, [], DL);
reset_dot_list(DL, ?EC_RESET_VALUES) ->
    lists:foldl(fun(#ec_dot{counter_max=Max}=DotX, Acc) -> [DotX#ec_dot{counter_min=Max, values=[]} | Acc] end, [], DL);
reset_dot_list(DL, ?EC_RESET_VALUES_ONLY) ->
    lists:foldl(fun(DotX, Acc) -> [DotX#ec_dot{values=[]} | Acc] end, [], DL).
			


    


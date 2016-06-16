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
	 find_module/1,
	 is_dirty/1,
	 delta_state_pair/1,
	 causal_consistent/4,
	 reset/3,
	 causal_history/3]).

-include("erlang_crdt.hrl").

-spec add_param(DVV :: #ec_dvv{}, State :: #ec_dvv{}) -> #ec_dvv{}.
add_param(DVV, #ec_dvv{module=Mod, type=Type, name=Name}) ->
    DVV#ec_dvv{module=Mod, type=Type, name=Name}.

-spec new_delta(Value :: term(), DL :: list(), State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.
new_delta(Value, DL, State, ServerId) ->
    NewDelta = ec_dvv:new(dot_list(DL, ServerId), Value),
    add_param(NewDelta#ec_dvv{status=?EC_DVV_DIRTY_DELTA}, State).

-spec reset(DVV :: #ec_dvv{},
	    ServerId :: term(),
	    Flag :: ?EC_RESET_NONE | 
		    ?EC_RESET_VALUES | 
		    ?EC_RESET_ANNONYMUS_LIST | 
		    ?EC_RESET_ALL) -> #ec_dvv{}.
reset(#ec_dvv{dot_list=DL}=DVV, ServerId, Flag) ->
    reset(DVV#ec_dvv{dot_list=dot_list(DL, ServerId)}, Flag).

-spec find_module(Type :: atom()) -> atom() | error.
find_module(Type) ->
    case maps:find(Type, ?EC_CRDT_MAP) of
	error     ->
	    error;
	{ok, Mod} ->
	    Mod
    end.

-spec is_dirty(DVV :: #ec_dvv{}) -> true | false.
is_dirty(#ec_dvv{status={?EC_DVV_DIRTY, _}}) ->
    true;
is_dirty(#ec_dvv{}) ->
    false.

-spec delta_state_pair({Delta :: #ec_dvv{}, State :: #ec_dvv{}}) -> {#ec_dvv{}, #ec_dvv{}}.
delta_state_pair({Delta, State}) ->
    case is_state(State) of
	true  ->
	    {Delta, State};
	false ->
	    {State, Delta}
    end.

-spec causal_consistent(Delta :: #ec_dvv{}, State :: #ec_dvv{}, ServerId :: term(), List :: list()) -> list().
causal_consistent(Delta,
		  State,
		  ServerId,
		  List) ->
    case ec_dvv:causal_consistent(Delta, State, ServerId) of
        ?EC_CAUSALLY_CONSISTENT -> 
	    List;
	Reason                  ->                 
	    [Reason | List]
    end.

-spec causal_history(State :: #ec_dvv{}, ServerId :: term(), Flag :: ?EC_CAUSAL_SERVER_ONLY | ?EC_CAUSAL_EXCLUDE_SERVER) -> #ec_dvv{} | ?EC_UNDEFINED.
causal_history(#ec_dvv{dot_list=DL}=State, ServerId, ?EC_CAUSAL_EXCLUDE_SERVER) ->
    NewDL = lists:foldl(fun(Dot, Acc) -> exclude_causal_history_fun(Dot, ServerId, Acc) end, [], DL),
    State#ec_dvv{dot_list=ec_dvv:sort_dot_list(NewDL), annonymus_list=[]};
causal_history(#ec_dvv{dot_list=DL}=State, ServerId, ?EC_CAUSAL_SERVER_ONLY) ->
    case dot_list(DL, ServerId) of
	[]    ->
	    ?EC_UNDEFINED;
	[Dot] ->
	    State#ec_dvv{dot_list=[ec_dvv:empty_dot(Dot)], annonymus_list=[]}
    end.

% private function

-spec reset(DVV :: #ec_dvv{},
            Flag :: ?EC_RESET_NONE |
                    ?EC_RESET_VALUES |
                    ?EC_RESET_ANNONYMUS_LIST |
                    ?EC_RESET_ALL) -> #ec_dvv{}.
reset(#ec_dvv{dot_list=DL}=DVV, ?EC_RESET_ANNONYMUS_LIST) ->    
    DVV#ec_dvv{dot_list=reset_dot_list(DL, ?EC_RESET_NONE), annonymus_list=[]};
reset(#ec_dvv{dot_list=DL}=DVV, ?EC_RESET_ALL) ->
    DVV#ec_dvv{dot_list=reset_dot_list(DL, ?EC_RESET_VALUES), annonymus_list=[]};
reset(#ec_dvv{dot_list=DL}=DVV, Flag) ->
    DVV#ec_dvv{dot_list=reset_dot_list(DL, Flag)}.

-spec reset_dot_list(DL :: list(), Flag :: ?EC_RESET_NONE | ?EC_RESET_VALUES) -> list().
reset_dot_list(DL, ?EC_RESET_NONE) ->
    lists:foldl(fun(#ec_dot{counter_max=Max}=DotX, Acc) -> [DotX#ec_dot{counter_min=Max+1} | Acc] end, [], DL);
reset_dot_list(DL, ?EC_RESET_VALUES) ->
    lists:foldl(fun(#ec_dot{counter_max=Max}=DotX, Acc) -> [DotX#ec_dot{counter_min=Max+1, values=[]} | Acc] end, [], DL).

-spec is_state(DVV :: #ec_dvv{}) -> true | false.
is_state(#ec_dvv{status={_, ?EC_DVV_STATE}}) ->	
    true;
is_state(#ec_dvv{}) ->
    false.

-spec dot_list(DL :: list(), ServerId :: term()) -> list().
dot_list(DL, ServerId) ->
    case ec_dvv:find_dot(DL, ServerId) of
	false ->	    
	    [];
	Dot   ->
            [Dot]
    end.

-spec exclude_causal_history_fun(Dot :: #ec_dot{}, ServerId :: term(), Acc :: list()) -> list().
exclude_causal_history_fun(#ec_dot{replica_id=ReplicaId}=Dot, ServerId, Acc) when ReplicaId =/= ServerId ->
    [ec_dvv:empty_dot(Dot) | Acc];
exclude_causal_history_fun(_, _, Acc) ->
    Acc.






    


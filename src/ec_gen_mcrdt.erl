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

-module(ec_gen_mcrdt).

-behavior(ec_gen_crdt).

-export([new_crdt/2,
	 delta_crdt/4,
	 reconcile_crdt/2,
	 update_fun_crdt/1,
	 merge_fun_crdt/1,
	 query_crdt/2,
	 reset_crdt/1,
	 causal_consistent_crdt/4,
	 add_gcounter/3,
	 add_pncounter/3,
	 merge_pncounter/3,
	 merge_gcounter/3,
	 enable_win/3,
	 disable_win/3]).

-include("erlang_crdt.hrl").

-spec new_crdt(Type :: atom(), Args :: term()) -> #ec_dvv{}.
new_crdt(Type, Args) ->
    #ec_dvv{module=?MODULE, type=Type, option=Args}.

-spec delta_crdt(Ops :: term(), DL :: list(), State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.
delta_crdt(Ops, DL, #ec_dvv{module=?MODULE, type=Type}=State, ServerId) ->
    ec_crdt_util:new_delta(new_value(Ops, Type), DL, State, ServerId).

-spec reconcile_crdt(State :: #ec_dvv{}, ServerId :: term()) -> #ec_dvv{}.
reconcile_crdt(#ec_dvv{module=?MODULE}=State, _ServerId) ->
    State.

-spec causal_consistent_crdt(Delta :: #ec_dvv{}, State :: #ec_dvv{}, Offset :: non_neg_integer(), ServerId :: term()) -> ?EC_CAUSALLY_CONSISTENT |
															 ?EC_CAUSALLY_AHEAD |
															 ?EC_CAUSALLY_BEHIND.
causal_consistent_crdt(#ec_dvv{module=?MODULE, type=Type, option=Option}=Delta, 
		       #ec_dvv{module=?MODULE, type=Type, option=Option}=State,
		       Offset,
		       ServerId) ->
    case Type of
	?EC_MVREGISTER ->
	    ec_dvv:causal_consistent(Delta, State, Offset, ServerId);
	?EC_EWFLAG     ->
	    ec_dvv:causal_consistent(Delta, State, Offset, ServerId);
	?EC_DWFLAG     ->
	    ec_dvv:causal_consistent(Delta, State, Offset, ServerId);
	?EC_GCOUNTER   ->
	    ec_dvv:causal_consistent(Delta, State, Offset, ServerId);
	?EC_PNCOUNTER  ->
	    ec_dvv:causal_consistent(Delta, State, Offset, ServerId)
    end.

-spec query_crdt(Criteria :: term(), State :: #ec_dvv{}) -> term().
query_crdt(_Criteria, #ec_dvv{module=?MODULE, type=Type}=State) ->
    Values = ec_dvv:values(State),
    case Type of
	?EC_MVREGISTER ->
	    Values;
	?EC_EWFLAG     ->
	    lists:foldl(fun(X, Flag) -> enable(X, Flag) end, false, Values);
	?EC_DWFLAG     ->
	    lists:foldl(fun(X, Flag) -> disable(X, Flag) end, true, Values);
        ?EC_GCOUNTER   ->
            lists:foldl(fun(PX, TPX) -> TPX+PX end, 0, Values);
	?EC_PNCOUNTER  ->
	    lists:foldl(fun({PX, NX}, TX) -> TX+PX-NX end, 0, Values)
    end.

-spec reset_crdt(State :: #ec_dvv{}) -> #ec_dvv{}.
reset_crdt(#ec_dvv{module=?MODULE, type=Type}=State) ->
    case Type of
	?EC_MVREGISTER ->
	    ec_crdt_util:reset(State, ?EC_RESET_ALL);
	?EC_EWFLAG     ->
	    ec_crdt_util:reset(State, ?EC_RESET_RETAIN_ALL);
	?EC_DWFLAG     ->
	    ec_crdt_util:reset(State, ?EC_RESET_RETAIN_ALL);
	?EC_PNCOUNTER  ->
	    ec_crdt_util:reset(State, ?EC_RESET_RETAIN_ALL);
	?EC_GCOUNTER   ->
	    ec_crdt_util:reset(State, ?EC_RESET_RETAIN_ALL)
    end. 

-spec update_fun_crdt(Args :: list()) -> {fun(), fun()}.
update_fun_crdt([Type]) ->
    case Type of
        ?EC_MVREGISTER ->
            {fun ec_dvv:merge_default/3,  fun ec_dvv:merge_default/3};
	?EC_EWFLAG     ->
	    {fun ?MODULE:enable_win/3,    fun ?MODULE:enable_win/3};
	?EC_DWFLAG     ->
	    {fun ?MODULE:disable_win/3,   fun ?MODULE:disable_win/3};
        ?EC_PNCOUNTER  ->
            {fun ?MODULE:add_pncounter/3, fun ?MODULE:add_pncounter/3};
        ?EC_GCOUNTER   ->
            {fun ?MODULE:add_gcounter/3,  fun ?MODULE:add_gcounter/3}
    end.

-spec merge_fun_crdt(Args :: list()) -> fun().
merge_fun_crdt([Type]) ->    
    case Type of
        ?EC_MVREGISTER ->
            {fun ec_dvv:merge_default/3,  fun ec_dvv:merge_default/3};
        ?EC_EWFLAG     ->
	    {fun ec_dvv:merge_default/3,  fun ec_dvv:merge_default/3};
        ?EC_DWFLAG     ->
	    {fun ec_dvv:merge_default/3,  fun ec_dvv:merge_default/3};
        ?EC_PNCOUNTER  ->
            {fun ?MODULE:add_pncounter/3, fun ?MODULE:merge_pncounter/3};
        ?EC_GCOUNTER   ->
            {fun ?MODULE:add_gcounter/3,  fun ?MODULE:merge_gcounter/3}
    end.

-spec add_gcounter(Value1 :: list(), Value2 :: list(), DefaultValue :: list()) -> list().
add_gcounter([Value1], [Value2], _DefaultValue) ->
    [Value1+Value2];
add_gcounter([], [Value2], _DefaultValue) ->
    [Value2];
add_gcounter([Value1], [], _DefaultValue) ->
    [Value1];
add_gcounter([], [], _DefaultValue) ->
    [0].

-spec merge_gcounter(Value1 :: list(), Value2 :: list(), DefaultValue :: list()) -> list().
merge_gcounter([Value1], [Value2], _DefaultValue) ->    
    [max(Value1, Value2)];
merge_gcounter([], [Value2], _DefaultValue) ->
    [Value2];
merge_gcounter([Value1], [], _DefaultValue) ->
    [Value1];
merge_gcounter([], [], _DefaultValue) ->
    [0].

-spec add_pncounter(Value1 :: list(), Value2 :: list(), DefaultValue :: list()) -> list().
add_pncounter([{PValue1, NValue1}], [{PValue2, NValue2}], _DefaultValue) ->
    [{PValue1+PValue2, NValue1+NValue2}];
add_pncounter([], [{PValue2, NValue2}], _DefaultValue) ->
    [{PValue2, NValue2}];
add_pncounter([{PValue1, NValue1}], [], _DefaultValue) ->
    [{PValue1, NValue1}];
add_pncounter([], [], _DefaultValue) ->
    [{0, 0}].

-spec merge_pncounter(Value1 :: list(), Value2 :: list(), DefaultValue :: list()) -> list().
merge_pncounter([{PValue1, NValue1}], [{PValue2, NValue2}], _DefaultValue) ->
    [{max(PValue1, PValue2), max(NValue1, NValue2)}];
merge_pncounter([], [{PValue2, NValue2}], _DefaultValue) ->
    [{PValue2, NValue2}];
merge_pncounter([{PValue1, NValue1}], [], _DefaultValue) ->
    [{PValue1, NValue1}];
merge_pncounter([], [], _DefaultValue) ->
    [{0, 0}].

-spec enable_win(Value1 :: list(), Value2 :: list(), DefaultValue :: list()) -> list().
enable_win([Value1], [Value2], _DefaultValue) ->
    [enable(Value1, Value2)];
enable_win(_Value1, _Value2, DefaultValue) ->
    DefaultValue.

-spec disable_win(Value1 :: list(), Value2 :: list(), DefaultValue :: list()) -> list().
disable_win([Value1], [Value2], _DefaultValue) ->
    [disable(Value1, Value2)];
disable_win(_Value1, _Value2, DefaultValue) ->
    DefaultValue.

% private function

-spec new_value(Ops :: term(), Type :: atom()) -> term().
new_value({Tag, Value}, Type) ->
    case {Type, Tag} of
	{?EC_MVREGISTER, value} ->
	    Value;
	{?EC_EWFLAG, value}     ->
	    Value;
	{?EC_DWFLAG, value}     ->
	    Value;
	{?EC_GCOUNTER, inc}     ->
	    Value;
	{?EC_PNCOUNTER, inc}    ->
	    {Value, 0};
	{?EC_PNCOUNTER, dec}    ->
	    {0, Value}
    end.

-spec enable(Value1 :: true | false, Value2 :: true | false) -> true | false.
enable(Value1, Value2) ->
    Value1 orelse Value2.

-spec disable(Value1 :: true | false, Value2 :: true | false) -> true | false.
disable(Value1, Value2) ->
    Value1 andalso Value2.

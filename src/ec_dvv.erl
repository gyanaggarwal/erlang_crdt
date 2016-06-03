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

-module(ec_dvv).

-export([new/1,
	 new/2,
         sync/2,
	 update/2,
	 update/3,
	 update/4,
	 values/1,
	 join/1,
	 causal_consistent/4,
	 compare_causality/2,
	 is_valid/1,
	 is_clean/1,
	 is_dirty/1,
	 sort_dot_list/1,
	 replace_dot_list/2,
	 merge_default/3]).

-include("erlang_crdt.hrl").

-spec new(V :: term()) -> #ec_dvv{}.
new(V) when is_list(V) ->
    #ec_dvv{annonymus_list=V};
new(V) ->
    new([V]).

-spec new(DL :: list() | ?EC_UNDEFINED, V :: term()) -> #ec_dvv{}.
new(?EC_UNDEFINED, V) ->
    new(V);
new(DL, V) when is_list(V) ->
    #ec_dvv{dot_list=sort_dot_list(normalized_dot_list(DL)), annonymus_list=V};
new(DL, V) ->
    new(DL, [V]).

-spec sync(Clocks :: list(), MergeFun :: {fun(), fun()}) -> #ec_dvv{}.
sync(Clocks, MergeFun) ->
    sync1(Clocks, MergeFun, ?EC_MERGE_DVV).

-spec update(Clock :: #ec_dvv{}, Id :: term()) -> #ec_dvv{}.
update(#ec_dvv{dot_list=DL, annonymus_list=[V]}, Id) ->
    #ec_dvv{dot_list=dot(DL, Id, V, {fun merge_default/3, fun merge_default/3})}.

-spec update(UClock :: #ec_dvv{}, LClock :: #ec_dvv{} | ?EC_UNDEFINED, Id :: term()) -> #ec_dvv{}.
update(UClock, LClock, Id) ->
    update(UClock, LClock, {fun merge_default/3, fun merge_default/3}, Id).

-spec update(UClock :: #ec_dvv{}, LClock :: #ec_dvv{} | ?EC_UNDEFINED, UpdateFun :: {fun(), fun()}, Id :: term()) -> #ec_dvv{}.
update(UClock, ?EC_UNDEFINED, _UpdateFun, Id) ->
    update(UClock, Id);
update(#ec_dvv{annonymus_list=[UV]}=UClock, LClock, UpdateFun, Id) ->
    {#ec_dvv{dot_list=DL, annonymus_list=AL}, _, ?EC_MERGE_DVV} = sync2(UClock#ec_dvv{annonymus_list=[]}, {LClock, UpdateFun, ?EC_MERGE_DVV}),
    #ec_dvv{dot_list=dot(DL, Id, UV, UpdateFun), annonymus_list=AL}.

-spec values(Clock :: #ec_dvv{} | ?EC_UNDEFINED) -> list() | ?EC_UNDEFINED.
values(?EC_UNDEFINED) ->
    ?EC_UNDEFINED;
values(#ec_dvv{dot_list=DL, annonymus_list=AL}) ->
    lists:foldl(fun(#ec_dot{values=Vs}, Acc) -> Acc ++ Vs end, AL, DL).

-spec join(Clock :: #ec_dvv{} | ?EC_UNDEFINED) -> list() | ?EC_UNDEFINED.
join(?EC_UNDEFINED) ->
    ?EC_UNDEFINED;
join(#ec_dvv{dot_list=DL}) ->
    lists:foldl(fun(Dot, Acc) -> [Dot#ec_dot{values=[]} | Acc] end, [], DL).

-spec causal_consistent(Clock1 :: #ec_dvv{} | ?EC_UNDEFINED, 
			Clock2 :: #ec_dvv{} | ?EC_UNDEFINED, 
			OffSet :: non_neg_integer(), 
			ServerId :: term()) -> ?EC_CAUSALLY_CONSISTENT | ?EC_CAUSALLY_BEHIND | ?EC_CAUSALLY_AHEAD.
causal_consistent(?EC_UNDEFINED, ?EC_UNDEFINED, _Offset, _ServerId) ->
    ?EC_CAUSALLY_CONSISTENT;
causal_consistent(?EC_UNDEFINED, #ec_dvv{}, _Offset, _ServerId) ->
    ?EC_CAUSALLY_BEHIND;
causal_consistent(#ec_dvv{}, ?EC_UNDEFINED, _Offset, _ServerId) ->
    ?EC_CAUSALLY_AHEAD;
causal_consistent(#ec_dvv{dot_list=DDL}, #ec_dvv{dot_list=SDL}, Offset, ServerId) ->
    case {lists:keyfind(ServerId, #ec_dot.replica_id, DDL), lists:keyfind(ServerId, #ec_dot.replica_id, SDL)} of
	{false, _}                                                               ->
	    ?EC_CAUSALLY_CONSISTENT;
	{#ec_dot{counter_min=1}, false}                                          ->
	    ?EC_CAUSALLY_CONSISTENT;
	{#ec_dot{}, false}                                                       ->
	    ?EC_CAUSALLY_AHEAD;
	{#ec_dot{counter_max=Max1, counter_min=Min1}, #ec_dot{counter_max=Max2}} ->
	    case (Max2 >= Min1) of
		true  ->
		    case (Max2 =< (Max1-Offset)) of
			true  ->
			    ?EC_CAUSALLY_CONSISTENT;
			false ->
			    ?EC_CAUSALLY_BEHIND
	            end;
		false  ->
		    ?EC_CAUSALLY_AHEAD
	    end
    end.

-spec compare_causality(Clock1 :: #ec_dvv{}, Clock2 :: #ec_dvv{}) -> ?EC_EQUAL | ?EC_MORE | ?EC_LESS | ?EC_CONCURRENT.
compare_causality(#ec_dvv{dot_list=DL1}, #ec_dvv{dot_list=DL2}) ->
    compare_causality(sort_dot_list(DL1), sort_dot_list(DL2), ?EC_EQUAL).

-spec is_valid(Clock :: #ec_dvv{} | ?EC_UNDEFINED) -> true | false.
is_valid(?EC_UNDEFINED) ->
    false;
is_valid(#ec_dvv{dot_list=DL, annonymus_list=AL}) ->
    lists:foldl(fun(#ec_dot{values=VS}, Flag) -> Flag orelse length(VS) > 0 end, length(AL) > 0, DL).

-spec is_clean(Clock :: #ec_dvv{}) -> true | false.
is_clean(#ec_dvv{status=Status}) ->
    Status =:= ?EC_DVV_CLEAN.

-spec is_dirty(Clock :: #ec_dvv{}) -> true | false.
is_dirty(#ec_dvv{status=Status}) ->
    Status =:= ?EC_DVV_DIRTY.

-spec sort_dot_list(DotList :: list()) -> list().
sort_dot_list(DotList) ->			    
    lists:keysort(#ec_dot.replica_id, DotList).

-spec replace_dot_list(DotList :: list(), Dot :: #ec_dot{}) -> list().
replace_dot_list(DotList, Dot) ->
    replace_dot_list(sort_dot_list(DotList), Dot, false, []).

-spec merge_default(Value1 :: term(), Value2 ::term(), DefaultValue ::term()) -> term().
merge_default(_Value1, _Value2, DefaultValue) ->    
    DefaultValue.

%% private function

-spec replace_dot_list(DotList :: list(), Dot :: #ec_dot{}, InsertFlag :: true | false, Acc :: list()) -> list().
replace_dot_list([#ec_dot{replica_id=Id1}=Dot1 | TDL], #ec_dot{replica_id=Id2}=Dot2, false, Acc) when Id1 < Id2 ->
    replace_dot_list(TDL, Dot2, false, [Dot1 | Acc]);
replace_dot_list([#ec_dot{replica_id=Id1} | TDL], #ec_dot{replica_id=Id2}=Dot2, false, Acc) when Id1 =:= Id2 ->
    replace_dot_list(TDL, Dot2, true, [Dot2 | Acc]);
replace_dot_list([#ec_dot{replica_id=Id1} | _TDL]=DL, #ec_dot{replica_id=Id2}=Dot2, false, Acc) when Id1 > Id2 ->
    replace_dot_list(DL, Dot2, true, [Dot2 | Acc]);
replace_dot_list([Dot1 | TDL], Dot2, true, Acc) ->
    replace_dot_list(TDL, Dot2, true, [Dot1 | Acc]);
replace_dot_list([], Dot, false, Acc) ->
    replace_dot_list([], Dot, true, [Dot | Acc]);
replace_dot_list([], _Dot, true, Acc) ->
    sort_dot_list(Acc).

-spec normalized_dot_list(DotList :: list()) ->list().
normalized_dot_list(DotList) ->
    lists:foldl(fun(#ec_dot{counter_max=Max}=DotX, Acc) -> [DotX#ec_dot{counter_min=Max} | Acc] end, [], DotList).
			
-spec dot(DL :: list(), Id :: atom(), V :: term(), UpdateFun :: {fun(), fun()}) -> list().
dot(DL, Id, V, UpdateFun) ->
    dot(sort_dot_list(DL), Id, V, UpdateFun, true, []).

-spec dot(DL :: list(), Id :: atom(), V :: term(), UpdateFun :: {fun(), fun()}, InsertFlag :: true | false, Acc :: list()) -> list().
dot([#ec_dot{replica_id=Id, counter_max=Max, counter_min=Min, values=Values} | T], Id, V, {AFun, _}=UpdateFun,  true, Acc) ->
    Values1 = AFun([V], Values, [V | Values]),
    dot(T, Id, V, UpdateFun, false, [#ec_dot{replica_id=Id, counter_max=Max+1, counter_min=Min, values=Values1} | Acc]);
dot([#ec_dot{replica_id=Id1} | _T]=DL, Id, V, UpdateFun, true, Acc) when Id1 > Id ->
    dot(DL, Id, V, UpdateFun, false, [#ec_dot{replica_id=Id, counter_max=1, counter_min=1, values=[V]} | Acc]); 
dot([H | T], Id, V, UpdateFun, InsertFlag, Acc) ->
    dot(T, Id, V, UpdateFun, InsertFlag, [H | Acc]);
dot([], Id, V, _UpdateFun, true, Acc) ->
    lists:reverse([#ec_dot{replica_id=Id, counter_max=1, counter_min=1, values=[V]} | Acc]);
dot([], _Id, _V, _UpdateFun, false, Acc) ->
    lists:reverse(Acc).

-spec merge(Dot1 :: #ec_dot{}, Dot2 :: #ec_dot{}, MergeFun :: {fun(), fun()}, Flag :: ?EC_ADD_DVV | ?EC_MERGE_DVV) -> #ec_dot{}.
merge(#ec_dot{replica_id=Id, counter_max=Max1, counter_min=Min1, values=V1}=Dot1, 
      #ec_dot{replica_id=Id, counter_max=Max2, counter_min=Min2, values=V2}=Dot2,
      {AddFun, MergeFun},
      ?EC_MERGE_DVV) ->
    case Max1 >= Max2 of
	true  ->
	    V3 = lists:sublist(V1, Max1-Max2+length(V2)),
	    V4 = case Min1 >= Max2 of
		     true  ->
			 AddFun(V1, V2, V3);
		     false ->
			 MergeFun(V1, V2, V3)
		 end, 
	    Dot1#ec_dot{counter_min=min(Min1, Min2), values=V4};
	false ->
	    V3 = lists:sublist(V2, Max2-Max1+length(V1)),
	    V4 = case Min2 >= Max1 of
		     true  ->
			 AddFun(V1, V2, V3);
		     false ->
			 MergeFun(V1, V2, V3)
		 end,
	    Dot2#ec_dot{counter_min=min(Min1, Min2), values=V4}
    end.

-spec compare_causality(DL1 :: list(), DL2 :: list(), Flag :: ?EC_EQUAL | ?EC_LESS | ?EC_MORE | ?EC_CONCURRENT) -> ?EC_EQUAL | ?EC_LESS | ?EC_MORE | ?EC_CONCURRENT.
compare_causality([#ec_dot{replica_id=Id, counter_max=C1} | T1], [#ec_dot{replica_id=Id, counter_max=C2} | T2], Flag) ->
    case C1 =:= C2 of
	true  ->
	    compare_causality(T1, T2, Flag);
	false ->
	    case {C1 < C2, Flag} of
		{true, ?EC_MORE}  ->
		    ?EC_CONCURRENT;
		{true, _}         ->
		    compare_causality(T1, T2, ?EC_LESS);
		{false, ?EC_LESS} ->
		    ?EC_CONCURRENT;
		{false, _}        ->
		    compare_causality(T1, T2, ?EC_MORE)
	    end
    end;
compare_causality([#ec_dot{replica_id=Id1} | _T1], [#ec_dot{replica_id=Id2} | _T2], ?EC_LESS) when Id1 < Id2 ->
    ?EC_CONCURRENT;
compare_causality([#ec_dot{replica_id=Id1} | T1], [#ec_dot{replica_id=Id2} | _T2]=C2, _Flag) when Id1 < Id2 ->
    compare_causality(T1, C2, ?EC_MORE);
compare_causality([#ec_dot{replica_id=Id1} | _T1], [#ec_dot{replica_id=Id2} | _T2], ?EC_MORE) when Id1 > Id2 ->
    ?EC_CONCURRENT;
compare_causality([#ec_dot{replica_id=Id1} | _T1]=C1, [#ec_dot{replica_id=Id2} | T2], _Flag) when Id1 > Id2 ->
    compare_causality(C1, T2, ?EC_LESS);
compare_causality([], [_H | _T], ?EC_MORE) -> 
    ?EC_CONCURRENT;
compare_causality([], [_H | _T], _Flag) ->
    ?EC_LESS;
compare_causality([_H | _T], [], ?EC_LESS) ->
    ?EC_CONCURRENT;
compare_causality([_H | _T], [], _Flag) ->
    ?EC_MORE;
compare_causality([], [], Flag) -> 
    Flag.

-spec sync1(Clocks :: list(), MergeFun :: {fun(), fun()}, Flag :: ?EC_ADD_DVV | ?EC_MERGE_DVV) -> #ec_dvv{}.
sync1(Clocks, MergeFun, Flag) ->     
    {DVV, MergeFun, Flag} = lists:foldl(fun sync2/2, {#ec_dvv{}, MergeFun, Flag}, Clocks),
    DVV.

-spec sync2(Clock1 :: #ec_dvv{}, {Clock2 :: #ec_dvv{}, MergeFun :: {fun(), fun()}, Flag :: ?EC_ADD_DVV | ?EC_MERGE_DVV}) -> #ec_dvv{}.
sync2(?EC_UNDEFINED, {Clock2, MergeFun, Flag}) ->
    {Clock2, MergeFun, Flag};
sync2(Clock1, {?EC_UNDEFINED, MergeFun, Flag}) ->
    {Clock1, MergeFun, Flag};
sync2(#ec_dvv{dot_list=[], annonymus_list=[]}, {Clock2, MergeFun, Flag}) ->
    {Clock2, MergeFun, Flag};
sync2(Clock1, {#ec_dvv{dot_list=[], annonymus_list=[]}, MergeFun, Flag}) ->
    {Clock1, MergeFun, Flag};
sync2(#ec_dvv{dot_list=DL1, annonymus_list=AL1}, {#ec_dvv{dot_list=DL2, annonymus_list=AL2}, MergeFun, Flag}) ->
    DLS1 = sort_dot_list(DL1),
    DLS2 = sort_dot_list(DL2),
    AL = case compare_causality(DLS1, DLS2, ?EC_EQUAL) of
	     ?EC_LESS ->
		 AL2;
	     ?EC_MORE ->
		 AL1;
	     _        ->
		 sets:to_list(sets:from_list(AL1++AL2))
	 end,
    {#ec_dvv{dot_list=sync3(DLS1, DLS2, MergeFun, Flag), annonymus_list=AL}, MergeFun, Flag}.

-spec sync3(DL1 :: list(), DL2 :: list(), MergeFun :: {fun(), fun()}, Flag :: ?EC_ADD_DVV | ?EC_MERGE_DVV) -> list().
sync3(DL1, DL2, MergeFun, Flag) ->    
    sync4(DL1, DL2, MergeFun, Flag, []).

-spec sync4(DL1 :: list(), DL2 :: list(), MergeFun :: {fun(), fun()}, Flag :: ?EC_ADD_DVV | ?EC_MERGE_DVV, Acc :: list()) -> list().
sync4([#ec_dot{replica_id=Id1}=H1 | T1]=DL1, [#ec_dot{replica_id=Id2}=H2 | T2]=DL2, MergeFun, Flag, Acc) ->
    case Id1 =:= Id2 of
        true  ->
            sync4(T1, T2, MergeFun, Flag, [merge(H1, H2, MergeFun, Flag) | Acc]);
	false ->            
	    case {Id1, Id2, Id1 < Id2} of
                {?EC_UNDEFINED, _, _} ->
                    sync4(T1, DL2, MergeFun, Flag, Acc);
                {_, ?EC_UNDEFINED, _} ->
                    sync4(DL1, T2, MergeFun, Flag, Acc);
                {_, _, true}          ->
                    sync4(T1, DL2, MergeFun, Flag, [H1 | Acc]);
                {_, _, false}         ->
                    sync4(DL1, T2, MergeFun, Flag, [H2 | Acc])
            end
    end;
sync4([], [H | T], MergeFun, Flag, Acc) ->
    sync4([], T, MergeFun, Flag, [H | Acc]);
sync4([H | T], [], MergeFun, Flag, Acc) ->
    sync4(T, [], MergeFun, Flag, [H | Acc]);
sync4([], [], _MergeFun, _Flag, Acc) ->
    lists:reverse(Acc).


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
	 sync/1,
	 update/2,
	 update/3,
	 values/1,
	 join/1,
	 reconcile/2]).

-include("erlang_crdt.hrl").

-spec new(V :: list() | term()) -> #ec_dvv{}.
new(V) when is_list(V) ->
    #ec_dvv{annonymus_list=V};
new(V) ->
    new([V]).

-spec new(DL :: list(), V :: list() | term()) -> #ec_dvv{}.
new(DL, V) when is_list(V) ->
    #ec_dvv{dot_list=sort_vv(DL), annonymus_list=V};
new(DL, V) ->
    new(DL, [V]).

-spec sync(Clocks :: list()) -> #ec_dvv{}.
sync(Clocks) ->   
    lists:foldl(fun sync/2, #ec_dvv{}, Clocks).

-spec update(Clock :: #ec_dvv{}, Id :: atom()) -> #ec_dvv{}.
update(#ec_dvv{dot_list=DL, annonymus_list=[V]}, Id) ->    
    #ec_dvv{dot_list=dot(DL, Id, V)}.

-spec update(UClock :: #ec_dvv{}, LClock :: #ec_dvv{}, Id :: atom()) -> #ec_dvv{}.
update(#ec_dvv{annonymus_list=[UV]}=UClock, LClock, Id) ->
    #ec_dvv{dot_list=DL, annonymus_list=AL} = sync(UClock#ec_dvv{annonymus_list=[]}, LClock),
    #ec_dvv{dot_list=dot(DL, Id, UV), annonymus_list=AL}.

-spec values(Clock :: #ec_dvv{}) -> list().
values(#ec_dvv{dot_list=DL, annonymus_list=AL}) ->
    lists:foldl(fun(#ec_dot{values=Vs}, Acc) -> Acc ++ Vs end, AL, DL).

-spec join(Clock :: #ec_dvv{}) -> list().
join(#ec_dvv{dot_list=DL}) ->
    lists:foldl(fun(Dot, Acc) -> [Dot#ec_dot{values=[]} | Acc] end, [], DL).

-spec reconcile(F :: fun((#ec_dvv{}) -> #ec_dvv{}), C :: #ec_dvv{}) -> #ec_dvv{}.
reconcile(F, C) ->
    F(C).

%% private function

-spec dot(DL :: list(), Id :: atom(), V :: term()) -> list().
dot(DL, Id, V) ->
    dot(sort_vv(DL), Id, V, true, []).

-spec dot(DL :: list(), Id :: atom(), V :: term(), InsertFlag :: true | false, Acc :: list()) -> list().
dot([#ec_dot{replica_id=Id, counter=Counter, values=Values} | T], Id, V, true, Acc) ->
    dot(T, Id, V, false, [#ec_dot{replica_id=Id, counter=Counter+1, values=[V | Values]} | Acc]);
dot([#ec_dot{replica_id=Id1} | _T]=DL, Id, V, true, Acc) when Id1 > Id ->
    dot(DL, Id, V, false, [#ec_dot{replica_id=Id, counter=1, values=[V]} | Acc]); 
dot([H | T], Id, V, InsertFlag, Acc) ->
    dot(T, Id, V, InsertFlag, [H | Acc]);
dot([], Id, V, true, Acc) ->
    lists:reverse([#ec_dot{replica_id=Id, counter=1, values=[V]} | Acc]);
dot([], _Id, _V, false, Acc) ->
    lists:reverse(Acc).

-spec sort_vv(VV :: list()) -> list().
sort_vv(VV) ->
    lists:keysort(2, VV).

-spec merge(Dot1 :: #ec_dot{}, Dot2 :: #ec_dot{}) -> #ec_dot{}.
merge(#ec_dot{replica_id=Id, counter=C1, values=V1}=Dot1, 
      #ec_dot{replica_id=Id, counter=C2, values=V2}=Dot2) ->
    case C1 >= C2 of
	true  ->
	    Dot1#ec_dot{values=lists:sublist(V1, C1-C2+length(V2))};
	false ->
	    Dot2#ec_dot{values=lists:sublist(V2, C2-C1+length(V1))}
    end.

-spec sync2(DL1 :: list(), DL2 :: list()) -> list().
sync2(DL1, DL2) ->
    sync2(DL1, DL2, []).

-spec sync2(DL1 :: list(), DL2 :: list(), Acc :: list()) -> list().
sync2([#ec_dot{replica_id=Id1}=H1 | T1]=DL1, [#ec_dot{replica_id=Id2}=H2 | T2]=DL2, Acc) ->
    case Id1 =:= Id2 of
	true  ->
	    sync2(T1, T2, [merge(H1, H2) | Acc]);
	false ->
	    case Id1 < Id2 of
		true  ->
		    sync2(T1, DL2, [H1 | Acc]);
		false ->
		    sync2(DL1, T2, [H2 | Acc])
	    end
    end;
sync2([], [H | T], Acc) ->
    sync2([], T, [H | Acc]);
sync2([H | T], [], Acc) ->
    sync2(T, [], [H | Acc]);
sync2([], [], Acc) ->
    lists:reverse(Acc).

-spec compare_causality(DL1 :: list(), DL2 :: list(), Flag :: ?EC_EQUAL | ?EC_LESS | ?EC_MORE | ?EC_CONCURRENT) -> ?EC_EQUAL | ?EC_LESS | ?EC_MORE | ?EC_CONCURRENT.
compare_causality([#ec_dot{replica_id=Id, counter=C1} | T1], [#ec_dot{replica_id=Id, counter=C2} | T2], Flag) ->
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

-spec sync(Clock1 :: #ec_dvv{}, Clock2 :: #ec_dvv{}) -> #ec_dvv{}.
sync(#ec_dvv{dot_list=[], annonymus_list=[]}, Clock2) ->
    Clock2;
sync(Clock1, #ec_dvv{dot_list=[], annonymus_list=[]}) ->
    Clock1;
sync(#ec_dvv{dot_list=DL1, annonymus_list=AL1}, #ec_dvv{dot_list=DL2, annonymus_list=AL2}) ->
    DLS1 = sort_vv(DL1),
    DLS2 = sort_vv(DL2),
    AL = case compare_causality(DLS1, DLS2, ?EC_EQUAL) of
	     ?EC_LESS ->
		 AL2;
	     ?EC_MORE ->
		 AL1;
	     _        ->
		 sets:to_list(sets:from_list(AL1++AL2))
	 end,
    #ec_dvv{dot_list=sync2(DLS1, DLS2), annonymus_list=AL}.

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

-module(ec_gen_crdt_test).

-compile(export_all).

-include("erlang_crdt.hrl").

new(Mod, Type) ->
    {ec_gen_crdt:new(Mod, Type), ec_gen_crdt:new(Mod, Type)}.

mutate([Ops | T], DL0, DI0, DV0, ServerId) ->
    case ec_gen_crdt:mutate(Ops, DL0, DI0, DV0, ServerId) of
	{error, _} ->
	    mutate(T, DL0, DI0, DV0, ServerId);
	{ok, DI1, DV1} ->
	    mutate(T, ec_dvv:join(DV1), DI1, DV1, ServerId)
    end;
mutate([], _, DI, DV, _) ->
    {DI, DV}.

data_mvregister() ->
    L11 = [{val, v111}],
    L13 = [{val, v131}, {val, v132}, {val, v133}],
    L15 = [{val, v151}, {val, v152}, {val, v153}, {val, v154}, {val, v155}],
    L25 = [{val, v231}, {val, v232}, {val, v233}, {val, v234}, {val, v255}],
    {ec_gen_mcrdt, ?EC_MVREGISTER, L11, L13, L15, L25}.

data_flag(Type, Value) ->    
    L11 = [{val, Value}],
    L13 = [{val, (not Value)}, {val, Value}, {val, (not Value)}],
    L15 = [{val, Value}, {val, (not Value)}, {val, Value}, {val, (not Value)}, {val, Value}],
    L25 = [{val, (not Value)}, {val, Value}, {val, (not Value)}, {val, Value}, {val, (not Value)}],
    {ec_gen_mcrdt, Type, L11, L13, L15, L25}.

data_gcounter() ->    
    L11 = [{inc, 111}],
    L13 = [{inc, 131}, {inc, 132}, {inc, 133}],
    L15 = [{inc, 151}, {inc, 152}, {inc, 153}, {inc, 154}, {inc, 155}],
    L25 = [{inc, 231}, {inc, 232}, {inc, 233}, {inc, 234}, {inc, 235}],
    {ec_gen_mcrdt, ?EC_GCOUNTER, L11, L13, L15, L25}.

data_pncounter() ->
    L11 = [{inc, 111}],
    L13 = [{inc, 131}, {dec, 132}, {inc, 133}],
    L15 = [{inc, 151}, {dec, 152}, {inc, 153}, {dec, 154}, {inc, 155}],
    L25 = [{dec, 231}, {inc, 232}, {dec, 233}, {inc, 234}, {dec, 235}],
    {ec_gen_mcrdt, ?EC_PNCOUNTER, L11, L13, L15, L25}.

data_orset01(Type) ->    
    L11 = [{rmv, v12}],
    L13 = [{add, v14}, {rmv, v14}, {add, v15}],
    L15 = [{add, v11}, {rmv, v11}, {add, v11}, {add, v12}, {add, v13}],
    L25 = [{add, v11}, {add, v12}, {add, v14}, {add, v16}, {rmv, v16}],
    {ec_gen_scrdt, Type, L11, L13, L15, L25}.

test1(Data) ->
    test1(Data, undefined).

test1({Mod, Type, L11, L13, L15, L25}, Criteria) ->
    % server x1
    {DI11, DV11} = new(Mod, Type),
    {DI12, DV12} = mutate(L15, ec_dvv:join(DV11), DI11, DV11, x1),                    % DI12 is delta mutation for elements 1,2,3,4,5
    {DI13, DV13} = mutate(L11, ec_dvv:join(DV11), ec_gen_crdt:reset(DI12), DV12, x1), % DI13 is delta mutation for element 6
    {DI14, DV14} = mutate(L13, ec_dvv:join(DV13), ec_gen_crdt:reset(DI13), DV13, x1), % DI14 is delta mutation for elements 7,8,9
    
    {DI15, DV15} = mutate(L11, ec_dvv:join(DV11), DI12, DV12, x1),                    % DI15 is delta mutation for elements 1,2,3,4,5,6
    {DI16, DV16} = mutate(L13, ec_dvv:join(DV15), DI15, DV15, x1),                    % DI16 is delta mutation for elements 1,2,3,4,5,6,7,8,9
    
    % server s2
    {DI21, DV21} = new(Mod, Type),
    {DI22, DV22} = mutate(L25, ec_dvv:join(DV21), DI21, DV21, s2),                    % DI22 is delta mutation for elements 1,2,3,4,5                    
    
    % updating s2 with incremental delta interval from x1
    {ok, DV23}   = ec_gen_crdt:merge(DI12, DV22),                                     % updating server s2 with delta mutation DI12 from x1
    {ok, DV24}   = ec_gen_crdt:merge(DI13, DV23),                                     % updating server s2 with delta mutation DI13 from x1
    {ok, DV25}   = ec_gen_crdt:merge(DI14, DV24),                                     % updating server s2 with delta mutation DI14 from x1
    
    % updating s2 with one consolidated delta interval from x1
    {ok, DV26}   = ec_gen_crdt:merge(DI16, DV22),                                     % updating server s2 with delta mutation DI16 from x1

    % updating x1 with delta interval from s2
    {ok, DV17}   = ec_gen_crdt:merge(DI22, DV16),                                     % updating server x1 with delta mutation DI22 from s2

    % checking causality
    R5           = ec_gen_crdt:merge(DI14, DV23),                                     % causally_ahead
    R6           = ec_gen_crdt:merge(DI13, DV25),                                     % causally_behind
    R7           = ec_gen_crdt:merge(DI14, DV25),                                     % causally_behind

    {DV25, ec_gen_crdt:query(Criteria, DV25),
     DV26, ec_gen_crdt:query(Criteria, DV26),
     DV17, ec_gen_crdt:query(Criteria, DV17),
     R5, R6, R7}.

test_orset01(Type) ->
    {UFun, DV11} = make_orset(Type),
    DV12 = mutate_orset({add, v11}, ec_dvv:join(DV11), DV11, UFun),
    DV13 = mutate_orset({add, v12}, ec_dvv:join(DV12), DV12, UFun),
    DV14 = mutate_orset({add, v13}, ec_dvv:join(DV13), DV13, UFun),
    DV15 = mutate_orset({rmv, v11}, ec_dvv:join(DV14), DV14, UFun),
    DV16 = mutate_orset({add, v11}, ec_dvv:join(DV15), DV15, UFun),
    ok.
 
data_concurrent_orset01() ->
    [{add, v11}, {add, v12}, {rmv, v11}, {add, v11}, {rmv, v12}, {add, v12}].

data_concurrent_orset02() ->    
    [{add, v11}, {add, v12}, {add, v13}, {rmv, v13}, {add, v14}, {rmv, v14}].

data_concurrent_orset03() ->
    [{add, v11}, {add, v12}, {rmv, v11}, {add, v13}, {rmv, v12}, {add, v14}].

data_concurrent_orset04() ->
    [{add, v11}, {add, v12}, {add, v13}, {rmv, v11}, {add, v14}, {rmv, v12}].
    
test_orset02(Type, [Ops1, Ops2, Ops3, Ops4, Ops5, Ops6]) ->
    {UFun, DV11} = make_orset(Type),
    DV12 = mutate_orset(Ops1, ec_dvv:join(DV11), DV11, UFun),
    DV13 = mutate_orset(Ops2, ec_dvv:join(DV12), DV12, UFun),
    DV14 = mutate_orset(Ops3, ec_dvv:join(DV13), DV13, UFun),
    DV15 = mutate_orset(Ops4, ec_dvv:join(DV11), DV14, UFun), % concurrent
    DV16 = mutate_orset(Ops5, ec_dvv:join(DV15), DV15, UFun),
    DV17 = mutate_orset(Ops6, ec_dvv:join(DV11), DV16, UFun), % concurrent
    ok.

make_orset(Type) ->
    UFun = {fun ec_dvv:merge_default/3, fun ec_dvv:merge_default/3},
    DV11 = ec_gen_crdt:new(ec_gen_scrdt, Type),
    io:fwrite("dv11=~p~n", [ec_gen_crdt:query(DV11)]),
    {UFun, DV11}.

mutate_orset(Ops, DL11, DV11, UFun) ->
    DD11 = ec_gen_scrdt:delta_crdt(Ops, DL11, DV11, x1),
    DX12 = ec_crdt_util:add_param(ec_dvv:update(DD11, DV11, UFun, x1), DV11),
    DV12 = ec_crdt_util:add_param(ec_gen_scrdt:reconcile_crdt(DX12, x1, ?EC_RECONCILE_LOCAL), DV11),
    io:fwrite("~p=~p~n", [Ops, ec_gen_crdt:query(DV12)]),
    DV12.



    

    


                                                    

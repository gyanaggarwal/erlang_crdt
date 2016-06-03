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

new(Type, Name) ->
    {ec_gen_crdt:new(Type, Name), ec_gen_crdt:new(Type, Name)}.

mutate([Ops | T], Args, DL0, DI0, DV0, ServerId) ->
    case ec_gen_crdt:mutate(Ops, DL0, DI0, DV0, ServerId) of
	{error, _} ->
	    mutate(T, Args, DL0, DI0, DV0, ServerId);
	{ok, DI1, DV1} ->
	    mutate(T, Args, ec_gen_crdt:causal_list(Args, DV1), DI1, DV1, ServerId)
    end;
mutate([], _, _, DI, DV, _) ->
    {DI, DV}.

data_mvregister(Name) ->
    L11 = [{val, v111}],
    L13 = [{val, v131}, {val, v132}, {val, v133}],
    L15 = [{val, v151}, {val, v152}, {val, v153}, {val, v154}, {val, v155}],
    L25 = [{val, v231}, {val, v232}, {val, v233}, {val, v234}, {val, v255}],
    {?EC_MVREGISTER, Name, ?EC_UNDEFINED, L11, L13, L15, L25}.

data_flag(Type, Name, Value) ->    
    L11 = [{val, Value}],
    L13 = [{val, (not Value)}, {val, Value}, {val, (not Value)}],
    L15 = [{val, Value}, {val, (not Value)}, {val, Value}, {val, (not Value)}, {val, Value}],
    L25 = [{val, (not Value)}, {val, Value}, {val, (not Value)}, {val, Value}, {val, (not Value)}],
    {Type, Name, ?EC_UNDEFINED, L11, L13, L15, L25}.

data_gcounter(Name) ->    
    L11 = [{inc, 111}],
    L13 = [{inc, 131}, {inc, 132}, {inc, 133}],
    L15 = [{inc, 151}, {inc, 152}, {inc, 153}, {inc, 154}, {inc, 155}],
    L25 = [{inc, 231}, {inc, 232}, {inc, 233}, {inc, 234}, {inc, 235}],
    {?EC_GCOUNTER, Name, ?EC_UNDEFINED, L11, L13, L15, L25}.

data_pncounter(Name) ->
    L11 = [{inc, 111}],
    L13 = [{inc, 131}, {dec, 132}, {inc, 133}],
    L15 = [{inc, 151}, {dec, 152}, {inc, 153}, {dec, 154}, {inc, 155}],
    L25 = [{dec, 231}, {inc, 232}, {dec, 233}, {inc, 234}, {dec, 235}],
    {?EC_PNCOUNTER, Name, ?EC_UNDEFINED, L11, L13, L15, L25}.

data_orset01(Type) ->    
    L11 = [{rmv, v13}],
    L13 = [{add, v14}, {rmv, v14}, {add, v15}],
    L15 = [{add, v11}, {rmv, v11}, {add, v11}, {add, v12}, {add, v13}],
    L25 = [{add, v11}, {add, v12}, {add, v14}, {add, v16}, {rmv, v16}],
    {Type, L11, L13, L15, L25}.

data_orset02(Type, Name) ->     
    L11 = [{add, v12}],
    L13 = [{add, v14}, {rmv, v14}, {add, v15}],
    L15 = [{add, v11}, {rmv, v11}, {add, v11}, {add, v12}, {rmv, v12}],
    L25 = [{add, v11}, {add, v13}, {add, v14}, {add, v16}, {rmv, v16}],
    {Type, Name, ?EC_UNDEFINED, L11, L13, L15, L25}.

data_ormap01(Type, Name) ->     
    L11 = [{rmv, k12}],
    L13 = [{put, {k14, v141}}, {rmv, k14}, {put, {k15, v151}}],
    L15 = [{put, {k11, v111}}, {rmv, k11}, {put, {k11, v112}}, {put, {k12, v121}}, {put, {k13, v131}}],
    L25 = [{put, {k11, v211}}, {put, {k12, v221}}, {put, {k14, v241}}, {put, {k16, v261}}, {rmv, k16}],
    {Type, Name, ?EC_UNDEFINED, L11, L13, L15, L25}.

test1(Data) ->
    test1(Data, ?EC_UNDEFINED, ?EC_UNDEFINED).

test1(Data, HFun, QFun) ->
    test1(Data, ?EC_UNDEFINED, HFun, QFun).

test1({Type, Name, Args, L11, L13, L15, L25}, Criteria, HFun, QFun) ->
    HFun1 = get_fun(HFun),
    QFun1 = get_fun(QFun),

    % server x1
    {DI11, DV11} = new(Type, Name),
    {DI12, DV12} = mutate(L15, Args, ec_gen_crdt:causal_list(Args, DV11), DI11, DV11, x1),                    % DI12 is delta mutation for elements 1,2,3,4,5
    {DI13, DV13} = mutate(L11, Args, ec_gen_crdt:causal_list(Args, DV11), ec_gen_crdt:reset(DI12), DV12, x1), % DI13 is delta mutation for element 6
    {DI14, _V14} = mutate(L13, Args, ec_gen_crdt:causal_list(Args, DV13), ec_gen_crdt:reset(DI13), DV13, x1), % DI14 is delta mutation for elements 7,8,9
    
    {DI15, DV15} = mutate(L11, Args, ec_gen_crdt:causal_list(Args, DV11), DI12, DV12, x1),                    % DI15 is delta mutation for elements 1,2,3,4,5,6
    {DI16, DV16} = mutate(L13, Args, ec_gen_crdt:causal_list(Args, DV15), DI15, DV15, x1),                    % DI16 is delta mutation for elements 1,2,3,4,5,6,7,8,9
    
    % server s2
    {DI21, DV21} = new(Type, Name),
    {DI22, DV22} = mutate(L25, Args, ec_gen_crdt:causal_list(Args, DV21), DI21, DV21, s2),                    % DI22 is delta mutation for elements 1,2,3,4,5 
              
    % updating s2 with incremental delta interval from x1
    {ok, DV23}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI12), DV22),                                        % updating server s2 with delta mutation DI12 from x1
    {ok, DV24}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI13), DV23),                                        % updating server s2 with delta mutation DI13 from x1
    {ok, DV25}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI14), DV24),                                        % updating server s2 with delta mutation DI14 from x1
    
    % updating s2 with one consolidated delta interval from x1
    {ok, DV26}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI16), DV22),                                        % updating server s2 with delta mutation DI16 from x1

    % updating x1 with delta interval from s2
    {ok, DV17}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI22), DV16),                                        % updating server x1 with delta mutation DI22 from s2

    % checking causality
    R5           = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI14), DV23),                                        % delta interval causally_ahead
    R6           = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI13), DV25),                                        % delta interval causally_behind

    {HFun1(DV25), QFun1(ec_gen_crdt:query(Criteria, DV25)),
     HFun1(DV26), QFun1(ec_gen_crdt:query(Criteria, DV26)),
     HFun1(DV17), QFun1(ec_gen_crdt:query(Criteria, DV17)),
     R5, R6}.

data_orset01() ->
    [{add, v11}, {add, v12}, {rmv, v11}, {add, v11}, {rmv, v12}, {add, v12}].

data_orset02() ->    
    [{add, v11}, {add, v12}, {add, v13}, {rmv, v13}, {add, v14}, {rmv, v14}].

data_orset03() ->
    [{add, v11}, {add, v12}, {rmv, v11}, {add, v13}, {rmv, v12}, {add, v14}].

data_orset04() ->
    [{add, v11}, {add, v12}, {add, v13}, {rmv, v11}, {add, v14}, {rmv, v12}].
    
data_ormap01() ->
    [{put, {k11, v111}}, {put, {k11, v112}}, {put, {k12, v121}}, {put, {k12, v122}}, {put, {k13, v131}}, {put, {k14, v141}}].

data_ormap02() ->
    [{put, {k11, v111}}, {put, {k12, v121}}, {rmv, k11}, {put, {k11, v112}}, {put, {k13, v131}}, {rmv, k13}].

data_ormap03() ->    
    [{put, {k11, v111}}, {put, {k11, v112}}, {put, {k12, v121}}, {rmv, k11}, {rmv, k12}, {put, {k13, v131}}].

test_scrdt01(Type, QFun, [Ops1, Ops2, Ops3, Ops4, Ops5, Ops6]) ->
    {UFun, DV11} = make_scrdt(Type),
    DV12 = mutate_scrdt(Ops1, ec_dvv:join(DV11), DV11, UFun, QFun),
    DV13 = mutate_scrdt(Ops2, ec_dvv:join(DV12), DV12, UFun, QFun),
    DV14 = mutate_scrdt(Ops3, ec_dvv:join(DV13), DV13, UFun, QFun),
    DV15 = mutate_scrdt(Ops4, ec_dvv:join(DV11), DV14, UFun, QFun), % concurrent
    DV16 = mutate_scrdt(Ops5, ec_dvv:join(DV15), DV15, UFun, QFun),
    _V17 = mutate_scrdt(Ops6, ec_dvv:join(DV11), DV16, UFun, QFun), % concurrent
    ok.

make_scrdt(Type) ->
    UFun = {fun ec_dvv:merge_default/3, fun ec_dvv:merge_default/3},
    DV11 = ec_gen_crdt:new(Type),
    io:fwrite("dv11=~p~n", [ec_gen_crdt:query(DV11)]),
    {UFun, DV11}.

mutate_scrdt(Ops, DL11, DV11, UFun, QFun) ->
    DD11 = ec_gen_scrdt:delta_crdt(Ops, DL11, DV11, x1),
    DX12 = ec_crdt_util:add_param(ec_dvv:update(DD11, DV11, UFun, x1), DV11),
    DV12 = ec_crdt_util:add_param(ec_gen_scrdt:reconcile_crdt(DX12, x1, ?EC_RECONCILE_LOCAL), DV11),
    io:fwrite("~p=~p~n", [Ops, QFun(ec_gen_crdt:query(DV12))]),
    DV12.

hide_fun(_X) ->
    ok.

default_fun(X) ->
    X.

get_fun(?EC_UNDEFINED) ->
    fun default_fun/1;
get_fun(Fun) ->
    Fun.

get_ormap01() ->
    maps:from_list([{{x1,3,k11},sets:from_list([v112])},
		    {{x1,5,k13},sets:from_list([v131])},
		    {{x1,6,k13},sets:from_list([v132])},
		    {{x1,9,k15},sets:from_list([v152])},
		    {{x1,8,k15},sets:from_list([v151])},
		    {{s2,1,k11},sets:from_list([v211])},
		    {{s2,2,k12},sets:from_list([v221])},
		    {{s2,3,k14},sets:from_list([v241])}]).








    

    


                                                    

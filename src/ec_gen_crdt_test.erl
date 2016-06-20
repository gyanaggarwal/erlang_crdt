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

-define(REPLICAS, [r1, r2, r3]).

new(Type, Name) ->
    {ec_gen_crdt:new(Type, Name), ec_gen_crdt:new(Type, Name), ec_gen_crdt:new(Type, Name)}.

mutate([Ops | T], DM0, DI0, DV0, ServerId) ->
    case ec_gen_crdt:mutate(Ops, ec_gen_crdt:causal_context(Ops, DV0), DM0, DI0, DV0, ServerId) of
	{error, _} ->
	    mutate(T, DM0, DI0, DV0, ServerId);
	{ok, {DM1, DI1, DV1}} ->
	    mutate(T, ec_gen_crdt:reset(DM1, ServerId), DI1, DV1, ServerId)
    end;
mutate([], DM, DI, DV, _) ->
    {DM, DI, DV}.

data_mvregister(Name) ->
    L11 = [{val, v111}],
    L13 = [{val, v131}, {val, v132}, {val, v133}],
    L15 = [{val, v151}, {val, v152}, {val, v153}, {val, v154}, {val, v155}],
    L25 = [{val, v231}, {val, v232}, {val, v233}, {val, v234}, {val, v255}],
    {?EC_MVREGISTER, Name, L11, L13, L15, L25}.

data_flag(Type, Name, Value) ->    
    L11 = [{val, Value}],
    L13 = [{val, (not Value)}, {val, Value}, {val, (not Value)}],
    L15 = [{val, Value}, {val, (not Value)}, {val, Value}, {val, (not Value)}, {val, Value}],
    L25 = [{val, (not Value)}, {val, Value}, {val, (not Value)}, {val, Value}, {val, (not Value)}],
    {Type, Name, L11, L13, L15, L25}.

data_gcounter(Name) ->    
    L11 = [{inc, 111}],
    L13 = [{inc, 131}, {inc, 132}, {inc, 133}],
    L15 = [{inc, 151}, {inc, 152}, {inc, 153}, {inc, 154}, {inc, 155}],
    L25 = [{inc, 231}, {inc, 232}, {inc, 233}, {inc, 234}, {inc, 235}],
    {?EC_GCOUNTER, Name, L11, L13, L15, L25}.

data_pncounter(Name) ->
    L11 = [{inc, 111}],
    L13 = [{inc, 131}, {dec, 132}, {inc, 133}],
    L15 = [{inc, 151}, {dec, 152}, {inc, 153}, {dec, 154}, {inc, 155}],
    L25 = [{dec, 231}, {inc, 232}, {dec, 233}, {inc, 234}, {dec, 235}],
    {?EC_PNCOUNTER, Name, L11, L13, L15, L25}.

data_orset01(Type, Name) ->    
    L11 = [{rmv, v13}],
    L13 = [{add, v14}, {rmv, v14}, {add, v15}],
    L15 = [{add, v11}, {rmv, v11}, {add, v11}, {add, v12}, {add, v13}],
    L25 = [{add, v11}, {add, v12}, {add, v14}, {add, v16}, {rmv, v16}],
    {Type, Name, L11, L13, L15, L25}.

data_orset02(Type, Name) ->     
    L11 = [{add, v12}],
    L13 = [{add, v14}, {rmv, v11}, {add, v15}],
    L15 = [{add, v11}, {rmv, v11}, {add, v11}, {add, v12}, {rmv, v12}],
    L25 = [{add, v11}, {add, v13}, {add, v14}, {add, v16}, {rmv, v16}],
    {Type, Name, L11, L13, L15, L25}.

data_ormap01(Type, Name) ->     
    L11 = [{rmv, k12}],
    L13 = [{put, {k14, v141}}, {rmv, k14}, {put, {k15, v151}}],
    L15 = [{put, {k11, v111}}, {rmv, k11}, {put, {k11, v112}}, {put, {k12, v121}}, {put, {k13, v131}}],
    L25 = [{put, {k11, v211}}, {put, {k12, v221}}, {put, {k14, v241}}, {put, {k16, v261}}, {rmv, k16}],
    {Type, Name, L11, L13, L15, L25}.

data_compmap01() ->
    L11 = [{mutate, {{?EC_MVREGISTER, mvc1}, {val, v111}}}],
    L13 = [{mutate, {{?EC_PNCOUNTER,  pnc1}, {dec, 2}}}, 
	   {mutate, {{?EC_AWORSET,    awc1}, {add, v133}}}, 
	   {mutate, {{?EC_AWORSET,    awc1}, {rmv, v153}}}],
    L15 = [{mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 5}}}, 
	   {mutate, {{?EC_PNCOUNTER,  pnx1}, {inc, 6}}}, 
	   {mutate, {{?EC_AWORSET,    awc1}, {add, v153}}}, 
	   {mutate, {{?EC_AWORSET,    awx1}, {add, v154}}}, 
	   {mutate, {{?EC_MVREGISTER, mvc1}, {val, v155}}}],
    L25 = [{mutate, {{?EC_MVREGISTER, mvc1}, {val, v251}}}, 
	   {mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 4}}}, 
	   {mutate, {{?EC_AWORSET,    awc1}, {add, v253}}}, 
	   {mutate, {{?EC_PWORMAP,    pws2}, {put, {k11, v254}}}}, 
	   {mutate, {{?EC_PWORMAP,    pws2}, {put, {k12, v255}}}}],
    {?EC_COMPMAP, ?EC_UNDEFINED, L11, L13, L15, L25}. 

data_compmap02() ->    
    L11 = [{mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_MVREGISTER, mvc1}, {val, v111}}}}}],
    L13 = [{mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_PNCOUNTER,  pnc1}, {dec, 2}}}}},
           {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_AWORSET,    awc1}, {add, v133}}}}},
	   {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_AWORSET,    awc1}, {rmv, v153}}}}}],
    L15 = [{mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 5}}}}},
           {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_PNCOUNTER,  pnx1}, {inc, 6}}}}},
           {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_AWORSET,    awc1}, {add, v153}}}}},
           {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_AWORSET,    awx1}, {add, v154}}}}},
	   {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_MVREGISTER, mvc1}, {val, v155}}}}}],
    L25 = [{mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_MVREGISTER, mvc1}, {val, v251}}}}},
           {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 4}}}}},
           {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_AWORSET,    awc1}, {add, v253}}}}},
           {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_PWORMAP,    pws2}, {put, {k11, v254}}}}}},
	   {mutate, {{?EC_COMPMAP, cm01}, {mutate, {{?EC_PWORMAP,    pws2}, {put, {k12, v255}}}}}}],
    {?EC_COMPMAP, ?EC_UNDEFINED, L11, L13, L15, L25}.

data_compmap03() ->
    L11 = [{mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 1}}},
           {mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 2}}},
           {mutate, {{?EC_PNCOUNTER,  pnc2}, {inc, 3}}},
           {mutate, {{?EC_PNCOUNTER,  pnc2}, {inc, 4}}},
           {mutate, {{?EC_PNCOUNTER,  pnc3}, {inc, 5}}}],
    L13 = [{mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 1}}},
           {mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 2}}},
           {mutate, {{?EC_PNCOUNTER,  pnc2}, {inc, 3}}},
           {mutate, {{?EC_PNCOUNTER,  pnc3}, {inc, 4}}},
           {mutate, {{?EC_PNCOUNTER,  pnc3}, {inc, 5}}}],
    L15 = [{mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 1}}},
           {mutate, {{?EC_PNCOUNTER,  pnc3}, {inc, 2}}},
           {mutate, {{?EC_PNCOUNTER,  pnc2}, {inc, 3}}},
           {mutate, {{?EC_PNCOUNTER,  pnc2}, {inc, 4}}},
           {mutate, {{?EC_PNCOUNTER,  pnc3}, {inc, 5}}}],
    L25 = [{mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 1}}},
           {mutate, {{?EC_PNCOUNTER,  pnc1}, {inc, 2}}},
           {mutate, {{?EC_PNCOUNTER,  pnc2}, {inc, 3}}},
           {mutate, {{?EC_PNCOUNTER,  pnc2}, {inc, 4}}},
           {mutate, {{?EC_PNCOUNTER,  pnc3}, {inc, 5}}}],
    {?EC_COMPMAP, ?EC_UNDEFINED, L11, L13, L15, L25}.

data_compmap04() ->
    L11 = [{mutate, {{?EC_AWORSET,  pnc1}, {add, pnc1111}}},
           {mutate, {{?EC_AWORSET,  pnc1}, {add, pnc1112}}},
           {mutate, {{?EC_AWORSET,  pnc2}, {add, pnc2113}}},
           {mutate, {{?EC_AWORSET,  pnc2}, {add, pnc2114}}},
           {mutate, {{?EC_AWORSET,  pnc3}, {add, pnc3115}}}],
    L13 = [{mutate, {{?EC_AWORSET,  pnc1}, {add, pnc1121}}},
           {mutate, {{?EC_AWORSET,  pnc1}, {add, pnc1122}}},
           {mutate, {{?EC_AWORSET,  pnc2}, {add, pnc2123}}},
           {mutate, {{?EC_AWORSET,  pnc3}, {add, pnc3124}}},
	   {mutate, {{?EC_AWORSET,  pnc3}, {add, pnc3125}}}],
    L15 = [{mutate, {{?EC_AWORSET,  pnc1}, {add, pnc1131}}},
           {mutate, {{?EC_AWORSET,  pnc3}, {add, pnc3132}}},
           {mutate, {{?EC_AWORSET,  pnc2}, {add, pnc2133}}},
           {mutate, {{?EC_AWORSET,  pnc2}, {add, pnc2134}}},
	   {mutate, {{?EC_AWORSET,  pnc3}, {add, pnc3135}}}],
    L25 = [{mutate, {{?EC_AWORSET,  pnc1}, {add, pnc1211}}},
           {mutate, {{?EC_AWORSET,  pnc1}, {add, pnc1212}}},
           {mutate, {{?EC_AWORSET,  pnc2}, {add, pnc2213}}},
           {mutate, {{?EC_AWORSET,  pnc2}, {add, pnc2214}}},
	   {mutate, {{?EC_AWORSET,  pnc3}, {add, pnc3215}}}],
    {?EC_COMPMAP, ?EC_UNDEFINED, L11, L13, L15, L25}.

query_test0(Data) ->
    query_test0(Data, ?EC_UNDEFINED).

query_test0([{DV1, DV2, DV3} | _], Criteria) ->
    {ec_sets_util:pretty(ec_gen_crdt:query(Criteria, DV1)), 
     ec_sets_util:pretty(ec_gen_crdt:query(Criteria, DV2)), 
     ec_sets_util:pretty(ec_gen_crdt:query(Criteria, DV3))}.

run_test0({Type, Name, L11, L13, L15, L25}) ->
    % server x1
    {DM11, DI11, DV11} = new(Type, Name),
    {DM12, DI12, DV12} = mutate(L15, DM11, DI11, DV11, x1),                        % DI12 is delta mutation for elements 1,2,3,4,5
    {DM13, DI13, DV13} = mutate(L11, DM12, ec_gen_crdt:reset(DI12, x1), DV12, x1), % DI13 is delta mutation for element 6
    {_M14, DI14, _V14} = mutate(L13, DM13, ec_gen_crdt:reset(DI13, x1), DV13, x1), % DI14 is delta mutation for elements 7,8,9
    
    {DM15, DI15, DV15} = mutate(L11, DM12, DI12, DV12, x1),                        % DI15 is delta mutation for elements 1,2,3,4,5,6
    {_M16, DI16, DV16} = mutate(L13, DM15, DI15, DV15, x1),                        % DI16 is delta mutation for elements 1,2,3,4,5,6,7,8,9
    
    % server s2
    {DM21, DI21, DV21} = new(Type, Name),
    {_M22, DI22, DV22} = mutate(L25, DM21, DI21, DV21, s2),                        % DI22 is delta mutation for elements 1,2,3,4,5 
              
    % updating s2 with incremental delta interval from x1
    {ok, DV23}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI12), DV22, x1),         % updating server s2 with delta mutation DI12 from x1
    {ok, DV24}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI13), DV23, x1),         % updating server s2 with delta mutation DI13 from x1
    {ok, DV25}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI14), DV24, x1),         % updating server s2 with delta mutation DI14 from x1
    
    % updating s2 with one consolidated delta interval from x1
    {ok, DV26}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI16), DV22, x1),         % updating server s2 with delta mutation DI16 from x1

    % updating x1 with delta interval from s2
    {ok, DV17}   = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI22), DV16, s2),         % updating server x1 with delta mutation DI22 from s2

    % checking global causality
    R1           = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI14), DV23, x1),         % delta interval causally_ahead
    R2           = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI13), DV24, x1),         % delta interval causally_behind
    R3           = ec_gen_crdt:merge(ec_gen_crdt:mutated(DI13), DV25, x1),         % delta interval causally_behind

    [{DV25, DV26, DV17}, {R1, R2, R3}].

get_ormap01() ->
    maps:from_list([{{x1,3,k11},sets:from_list([v112])},
		    {{x1,5,k13},sets:from_list([v131])},
		    {{x1,6,k13},sets:from_list([v132])},
		    {{x1,9,k15},sets:from_list([v152])},
		    {{x1,8,k15},sets:from_list([v151])},
		    {{s2,1,k11},sets:from_list([v211])},
		    {{s2,2,k12},sets:from_list([v221])},
		    {{s2,3,k14},sets:from_list([v241])}]).

get_node_list(List) ->
    L1 = lists:dropwhile(fun(X) -> X =/= $@ end, atom_to_list(node())),
    lists:map(fun(X) -> list_to_atom(atom_to_list(X) ++ L1) end, List).

mutate01([N1, N2, N3 | _]) ->
    erlang_crdt:mutate(N1, {mutate, {{ec_aworset,   asc1}, {add, v11}}}),
    erlang_crdt:mutate(N2, {mutate, {{ec_aworset,   asc1}, {add, v11}}}),
    erlang_crdt:mutate(N3, {mutate, {{ec_aworset,   asc1}, {add, v11}}}),
    
    erlang_crdt:mutate(N1, {mutate, {{ec_aworset,   asc1}, {add, v21}}}),
    erlang_crdt:mutate(N2, {mutate, {{ec_aworset,   asc1}, {add, v22}}}),
    erlang_crdt:mutate(N3, {mutate, {{ec_aworset,   asc1}, {add, v23}}}),
    
    erlang_crdt:mutate(N1, {mutate, {{ec_pncounter, pcn1}, {inc, 50}}}),
    erlang_crdt:mutate(N2, {mutate, {{ec_pncounter, pcn2}, {dec, 30}}}),
    erlang_crdt:mutate(N3, {mutate, {{ec_pncounter, pcn3}, {inc, 40}}}),
    
    erlang_crdt:mutate(N1, {mutate, {{ec_pncounter, pcn1}, {dec, 10}}}),
    erlang_crdt:mutate(N2, {mutate, {{ec_pncounter, pcn2}, {inc, 60}}}),
    erlang_crdt:mutate(N3, {mutate, {{ec_pncounter, pcn3}, {dec, 20}}}).

mutate02([N1, N2, N3 | _]) ->
    erlang_crdt:mutate(N1, {mutate, {{ec_gcounter,  gcn1}, {inc, 50}}}),
    erlang_crdt:mutate(N2, {mutate, {{ec_gcounter,  gcn2}, {inc, 30}}}),
    erlang_crdt:mutate(N3, {mutate, {{ec_gcounter,  gcn3}, {inc, 40}}}),
    
    erlang_crdt:mutate(N1, {mutate, {{ec_gcounter,  gcn1}, {inc, 10}}}),
    erlang_crdt:mutate(N2, {mutate, {{ec_gcounter,  gcn2}, {inc, 60}}}),
    erlang_crdt:mutate(N3, {mutate, {{ec_gcounter,  gcn3}, {inc, 20}}}),
    
    erlang_crdt:stop(N3).

mutate03([N1, N2 | _]) ->
    erlang_crdt:mutate(N1, {mutate, {{ec_gcounter,  gcn5}, {inc, 50}}}),
    erlang_crdt:mutate(N2, {mutate, {{ec_gcounter,  gcn4}, {inc, 30}}}),
    
    erlang_crdt:mutate(N1, {mutate, {{ec_gcounter,  gcn1}, {inc, 10}}}),
    erlang_crdt:mutate(N2, {mutate, {{ec_gcounter,  gcn2}, {inc, 60}}}).

mutate04([_, N2 | _]) ->   
     erlang_crdt:mutate(N2, {mutate, {{ec_gcounter,  gcn4}, {inc, 30}}}),
     erlang_crdt:mutate(N2, {mutate, {{ec_gcounter,  gcn2}, {inc, 60}}}).

mutate05([N1 | _]) ->     
    erlang_crdt:mutate(N1, {mutate, {{ec_gcounter,  gcn4}, {inc, 30}}}),
    erlang_crdt:mutate(N1, {mutate, {{ec_gcounter,  gcn2}, {inc, 60}}}).

get_data(FileNameDM, FileNameDI) ->
    AppConfig = ec_crdt_config:get_env(),
    {ok, FileDM} = ec_storage_data_operation:open(FileNameDM),    
    {ok, FileDI} = ec_storage_data_operation:open(FileNameDI),
    {ok, DMQ}    = ec_storage_data_operation:read(AppConfig, FileDM),
    {ok, DIQ}    = ec_storage_data_operation:read(AppConfig, FileDI),
    ec_storage_data_operation:close(FileDM),
    ec_storage_data_operation:close(FileDI),
    {DM, DI, DS} = ec_data_util:get_data(DMQ, DIQ, {?EC_COMPMAP, ?EC_UNDEFINED}, node()),
    DI.

    

    





    
    




    
    
    
    










    

    


                                                    

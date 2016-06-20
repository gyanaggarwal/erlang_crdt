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

-module(ec_data_util).

-export([get_delta_interval/3,
	 get_data/4,
	 get_file_name/3]).

-include("erlang_crdt.hrl").

-spec get_delta_interval(Q0 :: queue:queue(), CH :: #ec_dvv{} | ?EC_UNDEFINED, ServerId :: term()) -> list().
get_delta_interval(Q0, CH, ServerId) ->
    get_delta_interval(Q0, CH, ServerId, false, []).

-spec get_data(DMQ0 :: queue:queue(), DIQ0 :: queue:queue(), CrdtSpec :: term(), SereverId :: term()) -> {#ec_dvv{}, #ec_dvv{}, #ec_dvv{}}.
get_data(DMQ0, DIQ0, {Type, Name}, ServerId) ->
    DS0 = ec_gen_crdt:new(Type, Name),
    DM0 = ec_gen_crdt:new(Type, Name),
    DI0 = case queue:out_r(DIQ0) of
	      {empty, _} ->
		  ec_gen_crdt:new(Type, Name);
	      {{value, DI}, _} ->
		  ec_gen_crdt:reset(DI, get_server_id(DI))
          end,
    get_data(DMQ0, ServerId, DM0, DI0, DS0, false).

-spec get_file_name(NodeId :: term(), DataDir :: string(), FileName :: string()) -> string().
get_file_name(NodeId, DataDir, FileName) ->
    NodeName = lists:takewhile(fun(X) -> X =/= $@ end, atom_to_list(NodeId)),
    DataDir ++ NodeName ++ FileName.
				       
% private function

-spec get_delta_interval(Q0 :: queue:queue(), CH :: #ec_dvv{} | ?EC_UNDEFINED, ServerId :: term(), Flag :: true | false, Acc :: list()) -> list().
get_delta_interval(Q0, CH, ServerId, Flag, Acc) -> 
    case {queue:out(Q0), Flag} of
	{{empty, _}, _}                                                             ->
	    lists:reverse(Acc);
	{{{value, #ec_dvv{dot_list=[#ec_dot{replica_id=ServerId}]}=DI}, Q1}, true}  ->
	    get_delta_interval(Q1, CH, ServerId, Flag, add_delta_interval(DI, Acc));
	{{{value, #ec_dvv{dot_list=[#ec_dot{replica_id=ServerId}]}=DI}, Q1}, false} ->
	    case ec_gen_crdt:causal_consistent(DI, CH, ServerId, ?EC_GLOBAL) of
		[] ->
		    get_delta_interval(Q1, CH, ServerId, true, add_delta_interval(DI, Acc));
		_  ->
		    get_delta_interval(Q1, CH, ServerId, Flag, Acc)
            end;
	{{{value, _}, Q1}, _}                                                       ->
	    get_delta_interval(Q1, CH, ServerId, Flag, Acc)
    end.

-spec add_delta_interval(DI :: #ec_dvv{}, Acc :: list()) -> list().
add_delta_interval(#ec_dvv{}=DI, Acc) ->
    [ec_gen_crdt:mutated(DI) | Acc].

-spec get_server_id(DI :: #ec_dvv{}) -> term().
get_server_id(#ec_dvv{dot_list=[#ec_dot{replica_id=ServerId}]}) -> 
    ServerId.

-spec get_data(DMQ0 :: queue:queue(), ServerId :: term(), DM0 :: #ec_dvv{}, DI0 :: #ec_dvv{}, DS0 :: #ec_dvv{}, Flag :: true | false) -> {#ec_dvv{}, #ec_dvv{}, #ec_dvv{}}.
get_data(DMQ0, ServerId, DM0, #ec_dvv{di_num=DINum}=DI0, DS0, Flag) ->
    case queue:out(DMQ0) of
	{empty, _}           ->
	    {DM0, DI0, DS0};
	{{value, DVX}, DMQ9} ->
	    ServerId1 = get_server_id(DVX),
	    {ok, DS9} = ec_gen_crdt:merge(DVX, DS0, ServerId1),
	    {DM9, DI9, Flag9} = case ServerId1 =:= ServerId of
				    false ->
					{DM0, DI0, Flag};
				    true  ->
					DMX3 = ec_gen_crdt:reset(DVX, ServerId),
					{DIX3, FlagX3} = case Flag of
							     true  ->
								 {ok, DIX1} = ec_gen_crdt:merge(DVX, DI0, ServerId),
								 {DIX1#ec_dvv{di_num=DINum}, Flag};
							     false ->
								 case ec_gen_crdt:causal_consistent(DVX, DI0, ServerId, ?EC_GLOBAL) of
								     [] ->
									 {ok, DIX2} = ec_gen_crdt:merge(DVX, DI0, ServerId),
									 {DIX2#ec_dvv{di_num=DINum}, true};
								     _  ->
									 {DI0, Flag}
								 end
                                                         end,
					{DMX3, DIX3, FlagX3}
				end,
	    get_data(DMQ9, ServerId, DM9, DI9, DS9, Flag9)
    end.
	    


    


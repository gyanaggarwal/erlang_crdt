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

-export([get_delta_interval/3]).

-include("erlang_crdt.hrl").

-spec get_delta_interval(Q0 :: queue:queue(), CH :: #ec_dvv{}, ServerId :: term()) -> list().
get_delta_interval(Q0, #ec_dvv{}=CH, ServerId) ->
    get_delta_interval(Q0, CH, ServerId, false, []).

% private function

-spec get_delta_interval(Q0 :: queue:queue(), CH :: #ec_dvv{}, ServerId :: term(), Flag :: true | false, Acc :: list()) -> list().
get_delta_interval(Q0, #ec_dvv{}=CH, ServerId, Flag, Acc) -> 
    case {queue:out(Q0), Flag} of
	{{empty, _}, _}            ->
	    lists:reverse(Acc);
	{{{value, DI}, Q1}, true}  ->
	    get_delta_interval(Q1, CH, ServerId, Flag, [DI | Acc]);
	{{{value, DI}, Q1}, false} ->
	    case ec_gen_crdt:causal_consistent(DI, CH, ServerId, ?EC_GLOBAL) of
		[] ->
		    get_delta_interval(Q1, CH, ServerId, true, [DI | Acc]);
		_  ->
		    get_delta_interval(Q1, CH, ServerId, Flag, Acc)
            end
    end.
 



    


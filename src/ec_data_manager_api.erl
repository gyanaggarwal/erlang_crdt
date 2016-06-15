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

-module(ec_data_manager_api).

-behavior(ec_data_manager).

-export([write_delta_mutation/1,
	 write_delta_interval/1,
	 read_delta_interval/2,
	 read_data/0]).

-include("erlang_crdt.hrl").

-spec write_delta_mutation(DM :: #ec_dvv{}) -> ok.
write_delta_mutation(#ec_dvv{}=DM) ->    
    gen_server:call(?EC_DATA_SERVER, {?EC_MSG_WRITE_DM, DM}).

-spec write_delta_interval(DI :: #ec_dvv{}) -> ok.
write_delta_interval(#ec_dvv{}=DI) ->
    gen_server:call(?EC_DATA_SERVER, {?EC_MSG_WRITE_DI, DI}).

-spec read_delta_interval(CH :: #ec_dvv{}, ServerId :: term()) -> list().
read_delta_interval(#ec_dvv{}=CH, ServerId) ->
    gen_server:call(?EC_DATA_SERVER, {?EC_MSG_READ_DI, {CH, ServerId}}).

-spec read_data() -> {#ec_dvv{}, #ec_dvv{}, #ec_dvv{}}.
read_data() ->
    gen_server:call(?EC_DATA_SERVER, ?EC_MSG_READ_DATA).






    


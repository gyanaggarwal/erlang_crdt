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

-module(ec_crdt_peer_api).

-export([merge/3, 
	 causal/3,
	 delta_interval/3]).

-include("erlang_crdt.hrl").

merge(NodeList, NodeId, #ec_dvv{}=Delta) ->
    gen_server:abcast(NodeList, ?EC_CRDT_SERVER, {?EC_MSG_MERGE, {NodeId, Delta}}).

causal(NodeList, NodeId, #ec_dvv{}=CausalDvv) ->
    gen_server:abcast(NodeList, ?EC_CRDT_SERVER, {?EC_MSG_CAUSAL, {NodeId, CausalDvv}}).

delta_interval(Node, NodeId, CausalList) ->
    gen_server:cast({?EC_CRDT_SERVER, Node}, {?EC_MSG_DELTA_INTERVAL, {NodeId, CausalList}}).







    


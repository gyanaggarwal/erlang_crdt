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

-module(ec_time_util).

-export([convert_to_milli_seconds/1,
	 get_random/1,
	 get_timeout/2,
	 get_current_time/0]).

-spec convert_to_milli_seconds({seconds | milli_seconds, {Min :: non_neg_integer(), Max :: non_neg_integer()}}) -> {non_neg_integer(), non_neg_integer()}.
convert_to_milli_seconds({seconds, {Min, Max}}) -> 
    {Min*1000, Max*1000};
convert_to_milli_seconds({milli_seconds, {Min, Max}}) ->
    {Min, Max}.

-spec get_random({Min :: non_neg_integer(), Max :: non_neg_integer()}) -> non_neg_integer().
get_random({Min, Max}) ->
    random:uniform(Max-Min)+Min.

-spec get_timeout(StartTime :: non_neg_integer(), TimeoutPeriod :: non_neg_integer()) -> non_neg_integer().
get_timeout(StartTime, TimeoutPeriod) ->
    max(0, (StartTime+TimeoutPeriod-erlang:system_time(milli_seconds))).

-spec get_current_time() -> non_neg_integer().
get_current_time() ->
    erlang:system_time(milli_seconds).






    


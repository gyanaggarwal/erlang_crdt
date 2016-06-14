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

-module(ec_data_server).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, 
	 handle_call/3, 
	 handle_cast/2, 
	 handle_info/2, 
	 code_change/3, 
	 terminate/2]).

-include("erlang_crdt.hrl").

start_link(AppConfig) ->
    gen_server:start_link({local, ?EC_DATA_SERVER}, ?MODULE, [AppConfig], []).

init([AppConfig]) ->
    State = #ec_data_state{app_config=AppConfig},
    {ok, State}.

handle_call({?EC_MSG_WRITE_DM, #ec_dvv{}=DM},
            _From,
            #ec_data_state{delta_mutation=DMQ}=State) ->
    {reply, ok, State#ec_data_state{delta_mutation=queue:in(DM, DMQ)}};
handle_call({?EC_MSG_WRITE_DI, #ec_dvv{}=DI},
            _From, 
            #ec_data_state{delta_interval=DIQ}=State) ->
    {reply, ok, State#ec_data_state{delta_interval=queue:in(DI, DIQ)}};
handle_call({?EC_MSG_READ_DI, {#ec_dvv{}=CH, ServerId}},
	    _From,
	    #ec_data_state{delta_interval=DIQ}=State) ->
    {reply, ec_data_util:get_delta_interval(DIQ, CH, ServerId), State}.

handle_cast({stop, Reason}, 
	    #ec_data_state{}=State) ->
    {stop, Reason, State};
handle_cast(_Msg, #ec_data_state{}=State) ->
    {noreply, State}.

handle_info(_Msg, #ec_data_state{}=State) ->
    {noreply, State}.

code_change(_OldVsn, #ec_data_state{}=State, _Extra) ->
    {ok, State}.

terminate(_Reason, #ec_data_state{}) ->
    ok.


      

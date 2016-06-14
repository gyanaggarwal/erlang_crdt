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

-module(ec_crdt_config).

-export([get_env/0, 
         get_node_id/1,
         get_timeout_period/1,
         get_data_manager/1,
	 get_data_dir/1,
         get_file_state_mutation/1,
	 get_file_delta_interval/1,
         get_debug_mode/1,
         get_sup_restart_intensity/1,
         get_sup_restart_period/1,
         get_sup_child_shutdown/1]).

-include("erlang_crdt.hrl").

-define(TIMEOUT_PERIOD,            {milli_seconds, {10000, 20000}}).
-define(DATA_MANAGER,              ec_data_manager_api).
-define(DATA_DIR,                  "./").
-define(FILE_STATE_MUTATION,       "_state.data").
-define(FILE_DELTA_INTERVAL,       "_delta.data").
-define(DEBUG_MODE,                false).
-define(SUP_RESTART_INTENSITY,     1).
-define(SUP_RESTART_PERIOD,        5).
-define(SUP_CHILD_SHUTDOWN,        2000).

-spec get_env() -> #ec_app_config{}.
get_env() ->
  #ec_app_config{node_id                  = node(),
                 timeout_period           = ec_time_util:convert_to_milli_seconds(ec_config:get_env(erlang_crdt, timeout_period, ?TIMEOUT_PERIOD)),
                 data_manager             = ec_config:get_env(erlang_crdt, data_manager,             ?DATA_MANAGER),
		 data_dir                 = ec_config:get_env(erlang_crdt, data_dir,                 ?DATA_DIR),
                 file_state_mutation      = ec_config:get_env(erlang_crdt, file_state_mutation,      ?FILE_STATE_MUTATION),
		 file_delta_interval      = ec_config:get_env(erlang_crdt, file_delta_interval,      ?FILE_DELTA_INTERVAL),
                 debug_mode               = ec_config:get_env(erlang_crdt, debug_mode,               ?DEBUG_MODE),
                 sup_restart_intensity    = ec_config:get_env(erlang_crdt, sup_restart_intensity,    ?SUP_RESTART_INTENSITY),
                 sup_restart_period       = ec_config:get_env(erlang_crdt, sup_restart_period,       ?SUP_RESTART_PERIOD),
                 sup_child_shutdown       = ec_config:get_env(erlang_crdt, sup_child_shutdown,       ?SUP_CHILD_SHUTDOWN)}.

-spec get_node_id(AppConfig :: #ec_app_config{}) -> atom().
get_node_id(#ec_app_config{node_id=NodeId}) ->
    NodeId.

-spec get_timeout_period(AppConfig :: #ec_app_config{}) -> {non_neg_integer(), non_neg_integer()}.
get_timeout_period(#ec_app_config{timeout_period=TimeoutPeriod}) ->
    TimeoutPeriod.

-spec get_data_manager(AppConfig :: #ec_app_config{}) -> atom().
get_data_manager(#ec_app_config{data_manager=DataManager}) ->
    DataManager.

-spec get_data_dir(AppConfig :: #ec_app_config{}) -> string().
get_data_dir(#ec_app_config{data_dir=DataDir}) ->
    DataDir.

-spec get_file_state_mutation(AppConfig :: #ec_app_config{}) -> string().
get_file_state_mutation(#ec_app_config{file_state_mutation=FileStateMutation}) ->
    FileStateMutation.

-spec get_file_delta_interval(AppConfig :: #ec_app_config{}) -> string().
get_file_delta_interval(#ec_app_config{file_delta_interval=FileDeltaInterval}) ->
    FileDeltaInterval.

-spec get_debug_mode(AppConfig :: #ec_app_config{}) -> boolean().
get_debug_mode(#ec_app_config{debug_mode=DebugMode}) ->
    DebugMode.

-spec get_sup_restart_intensity(AppConfig :: #ec_app_config{}) -> non_neg_integer().
get_sup_restart_intensity(#ec_app_config{sup_restart_intensity=SupRestartIntensity}) ->
    SupRestartIntensity.

-spec get_sup_restart_period(AppConfig :: #ec_app_config{}) -> non_neg_integer().
get_sup_restart_period(#ec_app_config{sup_restart_period=SupRestartPeriod}) ->
    SupRestartPeriod.

-spec get_sup_child_shutdown(AppConfig :: #ec_app_config{}) -> non_neg_integer().
get_sup_child_shutdown(#ec_app_config{sup_child_shutdown=SupChildShutdown}) ->
    SupChildShutdown.







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

-define(EC_GEN_MCRDT,                 ec_gen_mcrdt).
-define(EC_GEN_SCRDT,                 ec_gen_scrdt).
-define(EC_GEN_CCRDT,                 ec_gen_ccrdt).

-define(EC_MVREGISTER,                ec_mvregister).
-define(EC_GCOUNTER,                  ec_gcounter).
-define(EC_PNCOUNTER,                 ec_pncounter).
-define(EC_AWORSET,                   ec_aworset).
-define(EC_RWORSET,                   ec_rworset).
-define(EC_EWFLAG,                    ec_ewflag).
-define(EC_DWFLAG,                    ec_dwflag).
-define(EC_PWORMAP,                   ec_pwormap).
-define(EC_RWORMAP,                   ec_rwormap).
-define(EC_COMPMAP,                   ec_compmap).

-define(EC_CRDT_MAP,                  maps:from_list([{?EC_MVREGISTER, ?EC_GEN_MCRDT},
						      {?EC_GCOUNTER,   ?EC_GEN_MCRDT},
						      {?EC_PNCOUNTER,  ?EC_GEN_MCRDT},
						      {?EC_EWFLAG,     ?EC_GEN_MCRDT},
						      {?EC_DWFLAG,     ?EC_GEN_MCRDT},
						      {?EC_AWORSET,    ?EC_GEN_SCRDT},
						      {?EC_RWORSET,    ?EC_GEN_SCRDT},
						      {?EC_PWORMAP,    ?EC_GEN_SCRDT},
						      {?EC_RWORMAP,    ?EC_GEN_SCRDT},
						      {?EC_COMPMAP,    ?EC_GEN_CCRDT}])).

-define(EC_LESS,                      ec_less).
-define(EC_EQUAL,                     ec_equal).
-define(EC_MORE,                      ec_more).
-define(EC_CONCURRENT,                ec_concurrent).

-define(EC_ADD_DVV,                   ec_add_dvv).
-define(EC_MERGE_DVV,                 ec_merge_dvv).

-define(EC_RESET_NONE,                ec_reset_none).
-define(EC_RESET_VALUES,              ec_reset_values).
-define(EC_RESET_ANNONYMUS_LIST,      ec_reset_annonymus_list).
-define(EC_RESET_ALL,                 ec_reset_all).

-define(EC_OPS_VAL,                   val).
-define(EC_OPS_INC,                   inc).
-define(EC_OPS_DEC,                   dec).
-define(EC_OPS_ADD,                   add).
-define(EC_OPS_RMV,                   rmv).
-define(EC_OPS_PUT,                   put).
-define(EC_OPS_MUTATE,                mutate).

-define(EC_CAUSALLY_CONSISTENT,       causally_consistent).
-define(EC_CAUSALLY_BEHIND,           causally_behind).
-define(EC_CAUSALLY_AHEAD,            causally_ahead).

-define(EC_INVALID_OPERATION,         invalid_operation).

-define(EC_INCORRECT_DELTA_INTERVAL,  incorrect_delta_interval).
-define(EC_EMPTY_DELTA_INTERVAL,      empty_delta_interval).

-define(EC_LOCAL,                     ec_local).
-define(EC_GLOBAL,                    ec_global).

-define(EC_RECONCILE,                 ec_reconcile).
-define(EC_RECONCILED,                ec_reconciled).

-define(EC_DVV_CLEAN,                 dvv_clean).
-define(EC_DVV_DIRTY,                 dvv_dirty).
-define(EC_DVV_STATE,                 dvv_state).
-define(EC_DVV_DELTA,                 dvv_delta).
-define(EC_DVV_CLEAN_STATE,           {?EC_DVV_CLEAN, ?EC_DVV_STATE}).
-define(EC_DVV_DIRTY_STATE,           {?EC_DVV_DIRTY, ?EC_DVV_STATE}).
-define(EC_DVV_CLEAN_DELTA,           {?EC_DVV_CLEAN, ?EC_DVV_DELTA}).
-define(EC_DVV_DIRTY_DELTA,           {?EC_DVV_DIRTY, ?EC_DVV_DELTA}).

-define(EC_UNDEFINED,                 undefined).
-define(EC_ACTIVE,                    active).
-define(EC_INACTIVE,                  in_active).

-define(EC_CRDT_SERVER,               ec_crdt_server).
-define(EC_DATA_SERVER,               ec_data_server).

-define(EC_MSG_SETUP_REPL,            msg_setup_repl).
-define(EC_MSG_MUTATE,                msg_mutate).
-define(EC_MSG_QUERY,                 msg_query).
-define(EC_MSG_CAUSAL_CONTEXT,        msg_causal_context).
-define(EC_MSG_MERGE,                 msg_merge).
-define(EC_MSG_CAUSAL,                msg_causal).
-define(EC_MSG_DELTA_INTERVAL,        msg_delta_interval).
-define(EC_MSG_STOP,                  msg_stop).
-define(EC_MSG_RESUME,                msg_resume).

-record(ec_dot,            {replica_id=?EC_UNDEFINED   :: term() | ?EC_UNDEFINED,
			    counter_max=0              :: non_neg_integer(),
			    counter_min=0              :: non_neg_integer(),
			    values=[]                  :: list()}).

-record(ec_dvv,            {module                     :: atom(),
			    type                       :: atom(),
			    name                       :: atom(),
			    status=?EC_DVV_CLEAN_STATE :: term(),
			    dot_list=[]                :: list(),
			    annonymus_list=[]          :: list()}).

-record(ec_app_config,     {node_id                    :: atom(),
			    timeout_period=0           :: non_neg_integer(),
			    data_manager               :: atom(),
			    data_dir                   :: string(),
			    file_state_mutation        :: string(),
			    file_delta_interval        :: string(),
			    debug_mode=false           :: true | false,
			    sup_restart_intensity=100  :: non_neg_integer(),
			    sup_restart_period=1       :: non_neg_integer(),
			    sup_child_shutdown=2000    :: non_neg_integer()}).

-record(ec_crdt_state,     {status=?EC_INACTIVE        :: ?EC_INACTIVE | ?EC_ACTIVE,
			    replica_cluster            :: list(),
			    timeout_start              :: non_neg_integer(),
			    state_dvv                  :: #ec_dvv{},
			    delta_dvv                  :: #ec_dvv{},
			    app_config                 :: #ec_app_config{}}).

-record(ec_data_state,     {file_state_mutation        :: file:io_device(),
			    file_delta_interval        :: file:io_device(),
			    app_config                 :: #ec_app_config{}}).


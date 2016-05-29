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
-define(EC_RESET_VALUES_ONLY,         ec_reset_values_only).
-define(EC_RESET_RETAIN_ALL,          ec_reset_retain_all).

-define(EC_MVREGISTER,                ec_mvregister).
-define(EC_GCOUNTER,                  ec_gcounter).
-define(EC_PNCOUNTER,                 ec_pncounter).
-define(EC_AWORSET,                   ec_aworset).
-define(EC_RWORSET,                   ec_rworset).
-define(EC_EWFLAG,                    ec_ewflag).
-define(EC_DWFLAG,                    ec_dwflag).

-define(EC_OPS_VAL,                   val).
-define(EC_OPS_INC,                   inc).
-define(EC_OPS_DEC,                   dec).
-define(EC_OPS_ADD,                   add).
-define(EC_OPS_RMV,                   rmv).

-define(EC_CAUSALLY_CONSISTENT,       causally_consistent).
-define(EC_CAUSALLY_BEHIND,           causally_behind).
-define(EC_CAUSALLY_AHEAD,            causally_ahead).

-define(EC_INVALID_OPERATION,         invalid_operation).
-define(EC_DOT_DOES_NOT_EXIST,        dot_does_not_exist).           

-define(EC_INCORRECT_DELTA_INTERVAL,  incorrect_delta_interval).
-define(EC_EMPTY_DELTA_INTERVAL,      empty_delta_interval).

-define(EC_RECONCILE_LOCAL,           ec_reconcile_local).
-define(EC_RECONCILE_GLOBAL,          ec_reconcile_global).
-define(EC_RECONCILE,                 ec_reconcile).
-define(EC_RECONCILED,                ec_reconciled).

-record(ec_dot,            {replica_id=undefined   :: term() | undefined,
			    counter_max=0          :: non_neg_integer(),
			    counter_min=0          :: non_neg_integer(),
			    values=[]              :: list()}).

-record(ec_dvv,            {module                 :: atom(),
			    type                   :: atom(),
			    option                 :: term(),
			    dot_list=[]            :: list(),
			    annonymus_list=[]      :: list()}).


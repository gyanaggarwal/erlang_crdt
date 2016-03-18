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

-define(EC_LESS,           ec_less).
-define(EC_EQUAL,          ec_equal).
-define(EC_MORE,           ec_more).
-define(EC_CONCURRENT,     ec_concurrent).

-record(ec_dot,            {replica_id             :: atom(),
			    counter=0              :: non_neg_integer(),
			    values=[]              :: list()}).

-record(ec_dvv,            {dot_list=[]            :: list(),
			    annonymus_list=[]      :: list()}).


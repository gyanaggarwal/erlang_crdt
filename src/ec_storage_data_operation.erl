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

-module(ec_storage_data_operation).

-include("erlang_crdt.hrl").

-export([open/1,
         close/1,
         read/2,
         write/3]).

-spec open(FileName :: string()) -> {ok, file:io_device()} | {error, atom()}.
open(FileName) ->
    ec_storage_data_persist:open_data_file(FileName).
      
-spec close(File :: file:io_device()) -> ok | {error, atom()}.
close(File) ->
    case File of
	?EC_UNDEFINED ->
	    ok;
	_             ->
	    ec_storage_data_persist:close_data_file(File)
    end.

-spec read(AppConfig :: #ec_app_config{}, File :: file:io_device()) ->  {ok, queue:queue()} | {error, atom()}.
read(AppConfig, File) ->
    StorageData = ec_crdt_config:get_storage_data(AppConfig),
    read(StorageData, File, 0, queue:new()).

-spec read(StorageData :: atom(), File :: file:io_device(), Loc0 :: non_neg_integer(), Q0 :: queue:queue()) -> {ok, queue:queue()} | {error, atom()}.
read(StorageData, File, Loc0, Q0) ->
    case ec_storage_data_persist:read_data(File, Loc0, StorageData:header_byte_size()) of
	eof               -> 
	    {ok, Q0};
	{error, Reason}   -> 
	    {error, Reason};
	{ok, Loc1, HData} ->
	    DataSize =  StorageData:data_header(HData),
	    case ec_storage_data_persist:read_data(File, Loc1, DataSize) of
		eof               ->
		    ec_storage_data_persist:truncate_data(File, Loc0),
		    {error, ?EC_BAD_DATA};
		{error, Reason}   ->
		    {error, Reason};
		{ok, Loc2, RData} ->
		    case StorageData:binary_to_data(HData, RData) of
			{error, Reason} ->
			    eh_storage_data_persist:truncate_data(File, Loc0),
			    {error, Reason};
			{ok, Data}      ->
			    read(StorageData, File, Loc2, queue:in(Data, Q0))
                    end
            end
    end.

-spec write(AppConfig :: #ec_app_config{}, File :: file:io_device(), Data :: term()) -> ok | {error, atom()}.          
write(AppConfig, File, Data)->
    StorageData = ec_crdt_config:get_storage_data(AppConfig),
    Bin = StorageData:data_to_binary(Data),
    ec_storage_data_persist:write_data(File, Bin),
    file:sync(File).





    
  

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

-module(ec_storage_data_persist).

-export([open_data_file/1, 
         close_data_file/1, 
         write_data/2, 
         read_data/3, 
         truncate_data/2]).

-include_lib("kernel/include/file.hrl").

-spec open_data_file(string()) -> {ok, file:io_device()} | {error, atom()}.  
open_data_file(FileName) ->
  file:open(FileName, [append, read, binary, raw]).

-spec close_data_file(file:io_device()) -> ok | {error, atom()}.  
close_data_file(File) ->
  file:close(File).

-spec write_data(file:io_device(), binary()) -> ok | {error, atom()}.  
write_data(File, Data) ->
  file:write(File, Data). 
  
-spec read_data(file:io_device(), non_neg_integer(), non_neg_integer()) -> {ok, non_neg_integer(), binary()} | eof | {error, atom()}.  
read_data(File, Location, Size) ->
  case file:pread(File, Location, Size) of
    eof             ->
      eof;
    {error, Reason} ->
      {error, Reason};
    {ok, Data}      ->
      {ok, Location+byte_size(Data), Data}
  end.

 -spec truncate_data(file:io_device(), non_neg_integer()) -> ok | {error, atom()}.
 truncate_data(File, Location) ->
    {ok, _} = file:position(File, Location),
    file:truncate(File).



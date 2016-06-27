%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Copyright (c) 2015 Gyanendra Aggarwal.  All Rights Reserved.
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

-module(ec_storage_data_api).

-behavior(ec_storage_data).

-export([header_byte_size/0,
         data_to_binary/1,
	 binary_to_data/1,
         binary_to_data/2,
         data_header/1]).

-include("erlang_crdt.hrl").

-define(SHA_BYTE_SIZE,         20).
-define(DATA_BIT_SIZE,         32).

-spec header_byte_size() -> non_neg_integer().
header_byte_size() ->
    ?SHA_BYTE_SIZE+compute_bytes(?DATA_BIT_SIZE).

-spec data_to_binary(Data :: term()) -> binary().
data_to_binary(Data) ->
    Bin = term_to_binary(Data),
    BinSize = byte_size(Bin),

    Sha = compute_hash(BinSize, Bin),

    <<Sha:?SHA_BYTE_SIZE/binary, 
      BinSize:?DATA_BIT_SIZE, 
      Bin:BinSize/binary>>.

-spec binary_to_data(Bin :: binary()) -> {ok, term()} | {error, ?EC_BAD_DATA}.
binary_to_data(<<Sha:?SHA_BYTE_SIZE/binary, BinSize:?DATA_BIT_SIZE, Bin:BinSize/binary>>) ->
    case compute_hash(BinSize, Bin) of 
	Sha    -> 
	    {ok, binary_to_term(Bin)};
	_Other -> 
	    {error, ?EC_BAD_DATA}
    end.

-spec binary_to_data(Bin1 :: binary(), Bin2 :: binary()) -> {ok, term()} | {error, ?EC_BAD_DATA}.
binary_to_data(<<Sha:?SHA_BYTE_SIZE/binary, BinSize:?DATA_BIT_SIZE>>, Bin2) ->    
    case compute_hash(BinSize, Bin2) of
        Sha    ->
	     
            {ok, binary_to_term(Bin2)};
	        _Other -> 
            {error, ?EC_BAD_DATA}
    end.

-spec data_header(Bin :: binary()) -> non_neg_integer().
data_header(<<_Sha:?SHA_BYTE_SIZE/binary, BinSize:?DATA_BIT_SIZE>>) ->
    BinSize.
    
% private function

-spec compute_bytes(BitSize :: non_neg_integer()) -> non_neg_integer().
compute_bytes(BitSize) ->
    BitSize div 8.

-spec compute_hash(BinSize :: non_neg_integer(), Bin :: binary()) -> binary().
compute_hash(BinSize, Bin) ->
    B1 = <<BinSize:?DATA_BIT_SIZE, Bin:BinSize/binary>>,
    crypto:hash(sha, B1).


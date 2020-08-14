%% -------------------------------------------------------------------
%%
%% taken from: https://github.com/basho/riak_core/blob/develop/src/chash.erl
%%
%% chash: basic consistent hashing
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% -------------------------------------------------------------------

%% @doc A consistent hashing implementation.  The space described by the ring
%%      coincides with SHA-1 hashes, and so any two keys producing the same
%%      SHA-1 hash are considered identical within the ring.
%%
%%      Warning: It is not recommended that code outside this module make use
%%      of the structure of a chash.
%%
%% @reference Karger, D.; Lehman, E.; Leighton, T.; Panigrahy, R.; Levine, M.;
%% Lewin, D. (1997). "Consistent hashing and random trees". Proceedings of the
%% twenty-ninth annual ACM symposium on Theory of computing: 654~663. ACM Press
%% New York, NY, USA

-module(chash).

-define(CHASH_IMPL, application:getenv(riak_core, chash_impl, chash_legacy)).

-export([contains_name/2, fresh/2, index_to_int/1,
         int_to_index/1, lookup/2, key_of/1, members/1,
         merge_rings/2, next_index/2, nodes/1, node_size/2,
         offsets/1, predecessors/2, predecessors/3,
         preference_list/2, ring_increment/1, size/1,
         successors/2, successors/3, update/3]).

-export_type([chash/0, index/0, index_as_int/0]).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

-type chash() :: ?CHASH_IMPL:chash().

%% A Node is the unique identifier for the owner of a given partition.
%% An Erlang Pid works well here, but the chash module allows it to
%% be any term.
-type chash_node() :: '?CHASH_IMPL':chash_node().

%% Indices into the ring, used as keys for object location, are binary
%% representations of 160-bit integers.
-type index() :: '?CHASH_IMPL':index().

-type index_as_int() :: '?CHASH_IMPL':index_as_int().

-type node_entry() :: '?CHASH_IMPL':node_entry().

-type
     num_partitions() :: '?CHASH_IMPL':num_partitions().

-type
     preference_list() :: '?CHASH_IMPL':preference_list().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return true if named Node owns any partitions in the ring, else false.
-spec contains_name(Name :: chash_node(),
                    CHash :: chash()) -> boolean().

contains_name(Name, CHash) ->
    (?CHASH_IMPL):contains_name(Name, CHash).

%% @doc Create a brand new ring.  The size and seednode are specified;
%%      initially all partitions are owned by the seednode.  If NumPartitions
%%      is not much larger than the intended eventual number of
%%       participating nodes, then performance will suffer.
-spec fresh(NumPartitions :: num_partitions(),
            SeedNode :: chash_node()) -> chash().

fresh(NumPartitions, SeedNode) ->
    (?CHASH_IMPL):fresh(NumPartitions, SeedNode).

%% @doc Converts a given index to its integer representation.
-spec index_to_int(Index :: index()) -> Int ::
                                            integer().

index_to_int(Index) ->
    (?CHASH_IMPL):index_to_int(Index).

%% @doc Converts a given integer representation of an index to its original
%% form.
-spec int_to_index(Int :: integer()) -> Index ::
                                            index().

int_to_index(Int) -> (?CHASH_IMPL):int_to_index(Int).

%% @doc Find the Node that owns the partition identified by Index.
-spec lookup(Index :: index_as_int() | index(),
             CHash :: chash()) -> chash_node().

lookup(Index, CHash) ->
    (?CHASH_IMPL):lookup(Index, CHash).

%% @doc Given any term used to name an object, produce that object's key
%%      into the ring.  Two names with the same SHA-1 hash value are
%%      considered the same name.
-spec key_of(ObjectName :: term()) -> index().

key_of(ObjectName) -> (?CHASH_IMPL):key_of(ObjectName).

%% @doc Return all Nodes that own any partitions in the ring.
-spec members(CHash :: chash()) -> [chash_node()].

members(CHash) -> (?CHASH_IMPL):members(CHash).

%% @doc Return a randomized merge of two rings.
%%      If multiple nodes are actively claiming nodes in the same
%%      time period, churn will occur.  Be prepared to live with it.
-spec merge_rings(CHashA :: chash(),
                  CHashB :: chash()) -> chash().

merge_rings(CHashA, CHashB) ->
    (?CHASH_IMPL):merge_rings(CHashA, CHashB).

%% @doc Given the integer representation of a chash key,
%%      return the next ring index integer value.
-spec next_index(IntegerKey :: integer(),
                 CHash :: chash()) -> index_as_int().

next_index(IntegerKey, CHash) ->
    (?CHASH_IMPL):next_index(IntegerKey, CHash).

%% @doc Return the entire set of NodeEntries in the ring.
-spec nodes(CHash :: chash()) -> [node_entry()].

nodes(CHash) -> (?CHASH_IMPL):nodes(CHash).

%% @doc Returns the distance of the index of the segment belonging to the given
%% key to the index of the next segment
-spec node_size(Index :: index(),
                CHash :: chash()) -> index().

node_size(Index, CHash) ->
    (?CHASH_IMPL):node_size(Index, CHash).

%% @doc Return a list of section sizes as the index type.
-spec offsets(CHash :: chash()) -> [index()].

offsets(CHash) -> (?CHASH_IMPL):offsets(CHash).

%% @doc Given an object key, return all NodeEntries in reverse order
%%      starting at Index.
-spec predecessors(Index :: index() | index_as_int(),
                   CHash :: chash()) -> [node_entry()].

predecessors(Index, CHash) ->
    (?CHASH_IMPL):predecessors(Index, CHash).

%% @doc Given an object key, return the next N NodeEntries in reverse order
%%      starting at Index.
-spec predecessors(Index :: index() | index_as_int(),
                   CHash :: chash(), N :: integer()) -> [node_entry()].

predecessors(Index, CHash, N) ->
    (?CHASH_IMPL):predecessors(Index, CHash, N).

%% @doc Given an object key, return at least N NodeEntries in the order of
%% preferred replica placement.
-spec preference_list(Index :: index(),
                      CHash :: chash()) -> preference_list().

preference_list(Index, CHash) ->
    (?CHASH_IMPL):preference_list(Index, CHash).

%% @doc Return increment between ring indexes given
%% the number of ring partitions.
-spec ring_increment(NumPartitions ::
                         pos_integer()) -> pos_integer().

ring_increment(NumPartitions) ->
    (?CHASH_IMPL):ring_increment(NumPartitions).

%% @doc Return the number of partitions in the ring.
-spec size(CHash :: chash()) -> integer().

size(CHash) -> (?CHASH_IMPL):size(CHash).

%% @doc Given an object key, return all NodeEntries in order starting at Index.
-spec successors(Index :: index(),
                 CHash :: chash()) -> [node_entry()].

successors(Index, CHash) ->
    (?CHASH_IMPL):successors(Index, CHash).

%% @doc Given an object key, return the next N NodeEntries in order
%%      starting at Index.
-spec successors(Index :: index(), CHash :: chash(),
                 N :: integer()) -> [node_entry()].

successors(Index, CHash, N) ->
    (?CHASH_IMPL):successors(Index, CHash, N).

%% @doc Make the partition beginning at IndexAsInt owned by Name'd node.
-spec update(IndexAsInt :: index_as_int(),
             Name :: chash_node(), CHash :: chash()) -> chash().

update(IndexAsInt, Name, CHash) ->
    (?CHASH_IMPL):update(IndexAsInt, Name, CHash).

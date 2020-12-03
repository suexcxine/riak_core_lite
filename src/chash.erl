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

%%%-------------------------------------------------------------------
%%%
%%% taken from: https://github.com/basho/machi/blob/master/src/machi_chash.erl
%%%
%%% Copyright (c) 2007-2011 Gemini Mobile Technologies, Inc.  All rights reserved.
%%% Copyright (c) 2013-2015 Basho Technologies, Inc.  All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%%-------------------------------------------------------------------

%% @doc Consistent hashing library.  Also known as "random slicing".
%%
%% This code was originally from the Hibari DB source code at
%% [https://github.com/hibari]

-module(chash).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% chash API
-export([contains_name/2, fresh/1, change/2, lookup/2,
         lookup_node_entry/2, key_of/1, index_to_partition_id/2,
         make_handoff_chash/2, members/1, next_index/2, nodes/1,
         node_size/2, offsets/1, partition_id_to_index/2,
         predecessors/2, predecessors/3, ring_increment/1,
         size/1, successors/2, successors/3, update/3,
         diff_list/2, hash_is_partition_boundary/2]).

%% Owner for a range on the unit interval.  We are agnostic about its
%% type.
-type chash_node() :: term().

%% For this library, a weight is an integer which specifies the
%% capacity of a "owner" relative to other owners.  For example, if
%% owner A with a weight of 10, and if owner B has a weight of 20,
%% then B will be assigned twice as much of the unit interval as A.
-type weight() :: non_neg_integer().

%% A size map subdivides the range [0, hash:max_integer()], starting at 0, to
%% partitions that are assigned to various owners.  The sum of all
%% sizes must be exactly hash:max_integer().
-type size_map() :: [{chash_node(), non_neg_integer()}].

%% A index_list differs from a size_map in two respects: 1) index_list
%% contains tuples with the node name in 2nd position, 2) the index at each
%% position I_n > I_m, for all n, m such that n > m.
%% For example, a index_list of the size_map example for a hypothetic size of 16
%% [{node1, 4}, {node2, 8}, {node3, 4}] is
%% [{4, node1}, {12, node2}, {16, node3}].
-type index_list() :: [{index_as_int(), chash_node()}].

%% We can't use gb_trees:tree() because 'nil' (the empty tree) is
%% never valid in our case.  But teaching Dialyzer that is difficult.
-opaque index_tree() :: gb_trees:tree(index_as_int(),
                                      chash_node()).

%% A owner_weight_list is a mapping of owner name to their relative weight.
-type
     owner_weight_list() :: orddict:orddict(chash_node(),
                                            weight()).

%% Mapping of node to its difference in weight scaled to hash:max_integer()
-type diff_map() :: [{chash_node(), integer()}].

%% The chash type is a pair of a index_list and the according index tree
%% for an efficient query.
%% When the index tree is not up-to-date with the list it is stale instead.
-type chash() :: {index_list(), stale | index_tree()}.

%% Binary output of the hash function.
%% Size of hash:out_size().
-type index() :: binary().

-type index_as_int() :: non_neg_integer().

%% A single node is identified by its term and starting index.
-type node_entry() :: {index_as_int(), chash_node()}.

%% chash API
-export_type([chash/0, index/0, index_as_int/0,
              index_tree/0]).

%% ===================================================================
%% Public API
%% ===================================================================

%% =================== RANDOM SLICING ================================

%% @doc Create a size map, based on a basic owner weight list.
-spec make_size_map(owner_weight_list()) -> size_map().

make_size_map(NewOwnerWeights) ->
    make_size_map([], NewOwnerWeights).

%% @doc Create a size map, based on an older size map and a new weight
%% list.
%%
%% The weights in the new weight list may be different than (or the
%% same as) whatever weights were used to make the older size map.
-spec make_size_map(size_map(),
                    owner_weight_list()) -> size_map().

make_size_map([], NewOwnerWeights) ->
    make_size_map([{unused, hash:max_integer()}],
                  NewOwnerWeights);
make_size_map(OldSizeMap, NewOwnerWeights) ->
    NewSum = add_all_weights(NewOwnerWeights),
    %% Reconstruct old owner weights (should sum up to hash:max_integer())
    SumOldSizeDict = lists:foldl(fun ({Ch, Wt}, OrdDict) ->
                                         orddict:update_counter(Ch, Wt, OrdDict)
                                 end,
                                 orddict:new(), OldSizeMap),
    OldOwnerWeights = orddict:to_list(SumOldSizeDict),
    %% should be equal to hash:max_integer()
    OldSum = add_all_weights(OldOwnerWeights),
    OldChs = [Ch || {Ch, _} <- OldOwnerWeights],
    NewChs = [Ch || {Ch, _} <- NewOwnerWeights],
    OldChsOnly = OldChs -- NewChs,
    %% Mark any space by a deleted owner as unused.
    OldSizeMap2 = lists:map(fun ({Ch, Wt} = ChWt) ->
                                    case lists:member(Ch, OldChsOnly) of
                                      true -> {unused, Wt};
                                      false -> ChWt
                                    end
                            end,
                            OldSizeMap),
    %% Create a diff map of changing owners and added owners
    Factor = OldSum / NewSum,
    DiffMap = lists:map(fun ({Ch, NewWt}) ->
                                case orddict:find(Ch, SumOldSizeDict) of
                                  {ok, OldWt} ->
                                      % rounding errors leading to node indices
                                      % out of bounds worse than slight load
                                      % balancing issues caused by simply
                                      % truncating.
                                      {Ch, trunc(Factor * NewWt) - OldWt};
                                  error -> {Ch, trunc(Factor * NewWt)}
                                end
                        end,
                        NewOwnerWeights),
    make_size_map2(OldSizeMap2, DiffMap, NewOwnerWeights).

%% @doc Low-level function for querying a size tree: the (integer) point within
%%      the hash space.
%% @param Val Hash value to look up.
%% @param Tree Tree to query.
%% @param ZeroNode Node owning the index `0'. Needed for corner cases.
-spec query_tree(Val :: index() | index_as_int(),
                 Tree :: index_tree(),
                 ZeroNode :: chash_node()) -> node_entry().

query_tree(Val, Tree, ZeroNode) when is_integer(Val) ->
    chash_gb_next(Val rem hash:max_integer(), Tree,
                  ZeroNode);
query_tree(Val, Tree, ZeroNode) ->
    query_tree(hash:as_integer(Val), Tree, ZeroNode).

%% =================== CONSISTENT HASHING ============================

%% @doc Return true if named Node owns any partitions in the ring, else false.
-spec contains_name(Name :: chash_node(),
                    CHash :: chash()) -> boolean().

contains_name(Name, {IndexList, _}) ->
    lists:keymember(Name, 2, IndexList).

%% @doc Create a brand new ring. Initially each node contained in the weightlist
%% owns exactly one section sized according to their weight.
-spec fresh(WeightMap ::
                owner_weight_list()) -> chash().

fresh(WeightMap) ->
    {chash_size_map_to_index_list(make_size_map(WeightMap)),
     stale}.

-spec change(CHash :: chash(),
             WeightMap :: owner_weight_list()) -> chash().

change({IndexList, _}, WeightMap) ->
    {chash_size_map_to_index_list(make_size_map(chash_index_list_to_size_map(IndexList),
                                                WeightMap)),
     stale}.

%% @doc Find the Node that owns the partition identified by IndexAsInt.
%% Also Return the chash structure to make use of the lookup structure.
-spec lookup(Index :: index() | index_as_int(),
             CHash :: chash()) -> {chash_node(), chash()}.

lookup(Index, CHash) ->
    {{_, Node}, CHash2} = lookup_node_entry(Index, CHash),
    {Node, CHash2}.

%% @doc Find the Node Entry that owns the partition identified by IndexAsInt.
%% Returns the chash structure to make use of the lookup structure.
-spec lookup_node_entry(Index :: index() |
                                 index_as_int(),
                        CHash :: chash()) -> {node_entry(), chash()}.

lookup_node_entry(Index, CHash)
    when not is_integer(Index) ->
    lookup_node_entry(hash:as_integer(Index), CHash);
lookup_node_entry(Index, {NextList, stale}) ->
    lookup_node_entry(Index,
                      {NextList, chash_index_list_to_index_tree(NextList)});
lookup_node_entry(Index,
                  {[{0, ZeroNode} | _], FloatTree} = CHash) ->
    {query_tree(Index, FloatTree, ZeroNode), CHash}.

%% @doc Given any term used to name an object, produce that object's key
%%      into the ring.  Two names with the same SHA-1 hash value are
%%      considered the same name.
-spec key_of(ObjectName :: term()) -> index().

key_of(ObjectName) ->
    hash:hash(term_to_binary(ObjectName)).

%% @doc Return all Nodes that own any partitions in the ring.
-spec members(CHash :: chash()) -> [chash_node()].

members({NextList, _}) ->
    lists:usort([Name || {_, Name} <- NextList]).

%% @doc Given the integer representation of a chash key,
%%      return the next ring index integer value.
-spec next_index(IntegerKey :: integer(),
                 CHash :: chash()) -> index_as_int().

next_index(IntegerKey, {IndexList, stale}) ->
    next_index(IntegerKey,
               {IndexList, chash_index_list_to_index_tree(IndexList)});
next_index(IntegerKey,
           {[{0, ZeroNode} | _], IndexTree}) ->
    {Index, _Node} = query_tree(IntegerKey, IndexTree,
                                ZeroNode),
    Index.

%% @doc Return the entire set of NodeEntries in the ring.
-spec nodes(CHash :: chash()) -> [node_entry()].

nodes(CHash) -> {NextList, _} = CHash, NextList.

%% @doc Return the distance of the index of the segment belonging to the given
%% key to the index of the next segment
-spec node_size(Index :: index_as_int(),
                CHash :: chash()) -> pos_integer().

node_size(Index, {IndexList, _Tree})
    when is_integer(Index) ->
    node_size(Index, IndexList, 0);
node_size(Index, CHash) ->
    node_size(hash:as_integer(Index), CHash).

%% @doc Return a list of section sizes as integers.
-spec offsets(CHash :: chash()) -> [index_as_int()].

offsets({[_H | IndexList], _}) ->
    {Offsets, Start} = lists:foldl(fun ({I, _N}, {O, C}) ->
                                           {[I - C | O], I}
                                   end,
                                   {[], 0}, IndexList),
    lists:reverse([hash:max_integer() - Start | Offsets]).

%% @doc Given an object key, return all NodeEntries in reverse order
%%      starting at Index.
-spec predecessors(Index :: index() | index_as_int(),
                   CHash :: chash()) -> [node_entry()].

predecessors(Index, CHash) when not is_integer(Index) ->
    predecessors(hash:as_integer(Index), CHash);
predecessors(Index, CHash) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    %% Used to
    %% - find repair pairs in riak_core_vnode_manager
    predecessors(Index, CHash, chash:size(CHash)).

%% @doc Given an object key, return the next N NodeEntries in reverse order
%%      starting at Index.
-spec predecessors(Index :: index() | index_as_int(),
                   CHash :: chash(), N :: integer()) -> [node_entry()].

predecessors(Index, CHash, N)
    when not is_integer(Index) ->
    predecessors(hash:as_integer(Index), CHash, N);
predecessors(Index, CHash, N) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    %% Used to
    %% - find repair pairs in riak_core_vnode_manager
    Num = max_n(N, CHash),
    {Res, _} = lists:split(Num,
                           lists:reverse(ordered_from(Index, CHash))),
    Res.

%% @doc Return increment between ring indexes given
%% the number of ring partitions.
-spec ring_increment(NumPartitions ::
                         pos_integer()) -> 1.

ring_increment(_NumPartitions) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    %% Used to
    %% - determine partition ID in riak_core_ring_util
    %% - To help compute the future ring in riak_core_ring
    %% - Various test scenarios
    1.

%% @doc Return the number of partitions in the ring.
-spec size(CHash :: chash()) -> integer().

size({IndexList, _}) -> length(IndexList).

%% @doc Given an object key, return all NodeEntries in order starting at Index.
-spec successors(Index :: index() | index_as_int(),
                 CHash :: chash()) -> [node_entry()].

successors(Index, CHash) when not is_integer(Index) ->
    successors(hash:as_integer(Index), CHash);
successors(Index, CHash) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    %% Used
    %% - to find pairs involved in a repair operation
    successors(Index, CHash, chash:size(CHash)).

%% @doc Given an object key, return the next N NodeEntries in order
%%      starting at Index.
-spec successors(Index :: index() | index_as_int(),
                 CHash :: chash(), N :: integer()) -> [node_entry()].

successors(Index, CHash, N)
    when not is_integer(Index) ->
    successors(hash:as_integer(Index), CHash, N);
successors(Index, CHash, N) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    Num = max_n(N, CHash),
    Ordered = ordered_from(Index, CHash),
    NumPartitions = chash:size(CHash),
    if Num =:= NumPartitions -> Ordered;
       true -> {Res, _} = lists:split(Num, Ordered), Res
    end.

%% @doc Make the partition beginning at IndexAsInt owned by Name'd node.
-spec update(IndexAsInt :: index_as_int(),
             Name :: chash_node(), CHash :: chash()) -> chash().

%% TODO
%% Find usages and adapt them according to implementation in this module.
%% Used to
%% - renaming a node -> ok
%% - transferring a node to a partition
update(IndexAsInt, Name, CHash) ->
    {Nodes, _} = CHash,
    NewNodes = lists:keyreplace(IndexAsInt, 1, Nodes,
                                {IndexAsInt, Name}),
    {NewNodes, stale}.

%% @doc Create the intermediate chash structures containing indices of both the
%%      original and the future structure. The indices are owned by nodes such
%%      that there is no difference in ownership of keys on the structures.
%% @param OriginalCHash CHash structure of the ring handing off.
%% @param NextCHash CHash structure of the ring receiving a handoff.
%% @returns Tuple of both the original and next chash structure with same
%%          indices and adapted owners.
-spec make_handoff_chash(OriginalCHash :: chash(),
                         NextCHash :: chash()) -> {chash(), chash()}.

make_handoff_chash({OriginalIndexList, _},
                   {NextIndexList, _}) ->
    {HandoffOriginalIndexList, HandoffNextIndexList} =
        make_handoff_index_lists(OriginalIndexList,
                                 NextIndexList),
    {{HandoffOriginalIndexList, stale},
     {HandoffNextIndexList, stale}}.

%% @doc Compute a list of indices that have a different owner in the given chash
%%      structures. The structures need to have the same indices.
%% @param OldCHash CHash structure containing the old owners.
%% @param NewCHash CHash strcutrure containing the new owners.
%% @returns List of indices, the old owner and new owner.
-spec diff_list(OldCHash :: chash(),
                NewCHash :: chash()) -> [{index_as_int(), chash_node(),
                                          chash_node()}].

diff_list({OldIndexList, _}, {NewIndexList, _}) ->
    lists:filter(fun ({_, Old, New}) -> Old /= New end,
                 lists:zipwith(fun ({Index, OldOwner},
                                    {Index, NewOwner}) ->
                                       {Index, OldOwner, NewOwner}
                               end,
                               OldIndexList, NewIndexList)).

%% @doc Compute the index of the partition starting at 0 the key belongs to.
%% @param Key Key to compute the partition index for.
%% @param CHash Chash structure to compute the partition index on.
%% @returns Partition index.
-spec index_to_partition_id(Index :: index_as_int(),
                            CHash :: chash()) -> integer() | undefined.

index_to_partition_id(Index,
                      {[{0, _} | IndexList], _}) ->
    index_to_partition_id(hash:as_integer(Index), IndexList,
                          0).

%% @doc Compute the index owned by the node owning the partition with the given
%%      ID.
%% @param Partition Integer indicating the place of the partition in the index
%%        list, starting at 0.
%% @param CHash CHash structure to compute the index on.
%% @returns Integer index of the node owning the partition or undefined if the
%%          partition ID does not exist.
-spec partition_id_to_index(PartitionID :: integer(),
                            CHash :: chash()) -> index_as_int() | undefined.

partition_id_to_index(PartitionID,
                      {[{0, _} | IndexList], _}) ->
    L = length(IndexList) + 1,
    case {L < PartitionID, L =:= PartitionID} of
      {true, _} -> undefined;
      {_, true} -> 0;
      {false, false} -> lists:nth(PartitionID, IndexList)
    end.

-spec hash_is_partition_boundary(index() |
                                 index_as_int(),
                                 chash()) -> boolean().

hash_is_partition_boundary(CHashKey, CHash)
    when is_binary(CHashKey) ->
    CHashInt = hash:as_integer(CHashKey),
    hash_is_partition_boundary(CHashInt, CHash);
hash_is_partition_boundary(CHashInt, {IndexList, _}) ->
    lists:keymember(CHashInt, 1, IndexList).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% =================== CONSISTENT HASHING =============================

%% @private
%% @doc Return either N or the number of partitions in the ring, whichever
%%      is lesser.
-spec max_n(N :: integer(),
            CHash :: chash()) -> integer().

max_n(N, CHash) -> erlang:min(N, chash:size(CHash)).

%% @private
%% @doc Given an object key, return all NodeEntries in order starting at Index.
-spec ordered_from(Index :: index() | index_as_int(),
                   CHash :: chash()) -> [node_entry()].

ordered_from(Index, CHash) when not is_integer(Index) ->
    ordered_from(hash:as_integer(Index), CHash);
ordered_from(Index, {Nodes, _}) ->
    {A, B} = lists:foldl(fun ({I, N}, {L, G}) ->
                                 case I < Index of
                                   true -> {[{I, N} | L], G};
                                   false -> {L, [{I, N} | G]}
                                 end
                         end,
                         {[], []}, Nodes),
    lists:reverse(B) ++ lists:reverse(A).

%% @private
%% @doc Helper function to find the node size. Assumes that the IndexList is
%% ordered by index and the first entry has index 0.
-spec node_size(Index :: index_as_int(),
                IndexList :: index_list(),
                Start :: index_as_int()) -> integer().

node_size(_Index, [], Start) ->
    %% Index in last section
    hash:max_integer() - Start;
node_size(Index, [{I, _} | T], Start) ->
    case Index < I of
      true -> I - Start;
      false -> node_size(Index, T, I)
    end.

%% =================== RANDOM SLICING ================================

%% @private
%% @doc Assign gaps to nodes and merges neighbouring sections with the same
%% owner.
-spec make_size_map2(OldSizeMap :: size_map(),
                     DiffMap :: diff_map(),
                     NewOwnerWeights :: owner_weight_list()) -> size_map().

make_size_map2(OldSizeMap, DiffMap, _NewOwnerWeights) ->
    SizeMap = apply_diffmap(DiffMap, OldSizeMap),
    XX =
        combine_neighbors(collapse_unused_in_size_map(SizeMap)),
    XX.

%% @private
%% @doc Adapt the size map according to the given size differences
-spec apply_diffmap(DiffMap :: diff_map(),
                    SizeMap :: size_map()) -> size_map().

apply_diffmap(DiffMap, SizeMap) ->
    SubtractDiff = [{Ch, abs(Diff)}
                    || {Ch, Diff} <- DiffMap, Diff < 0],
    AddDiff = [D || {_Ch, Diff} = D <- DiffMap, Diff > 0],
    TmpSizeMap = iter_diffmap_subtract(SubtractDiff,
                                       SizeMap),
    iter_diffmap_add(AddDiff, TmpSizeMap).

%% @private
%% @doc Sum the weights of all nodes.
-spec add_all_weights(OwnerWeights ::
                          owner_weight_list()) -> integer().

add_all_weights(OwnerWeights) ->
    lists:foldl(fun ({_Ch, Weight}, Sum) -> Sum + Weight
                end,
                0, OwnerWeights).

%% @private
%% @doc Take sections or part of them away from each node such that they own the
%% desired size.
-spec iter_diffmap_subtract(DiffMap :: diff_map(),
                            SizeMap :: size_map()) -> size_map().

iter_diffmap_subtract([{Ch, Diff} | T], SizeMap) ->
    iter_diffmap_subtract(T,
                          apply_diffmap_subtract(Ch, Diff, SizeMap));
iter_diffmap_subtract([], SizeMap) -> SizeMap.

%% @private
%% @doc Assign sections or part of them to each node such that they own the
%% desired size.
-spec iter_diffmap_add(DiffMap :: diff_map(),
                       SizeMap :: size_map()) -> size_map().

iter_diffmap_add([{Ch, Diff} | T], SizeMap) ->
    iter_diffmap_add(T,
                     apply_diffmap_add(Ch, Diff, SizeMap));
iter_diffmap_add([], SizeMap) -> SizeMap.

%% @private
%% @doc Substract the size difference from sections owned by the node until the
%% desired size is reached.
-spec apply_diffmap_subtract(chash_node(), integer(),
                             size_map()) -> size_map().

apply_diffmap_subtract(Ch, Diff, [{Ch, Wt} | T]) ->
    if Wt == Diff -> [{unused, Wt} | T];
       Wt > Diff -> [{Ch, Wt - Diff}, {unused, Diff} | T];
       Wt < Diff ->
           [{unused, Wt} | apply_diffmap_subtract(Ch, Diff - Wt,
                                                  T)]
    end;
apply_diffmap_subtract(Ch, Diff, [H | T]) ->
    [H | apply_diffmap_subtract(Ch, Diff, T)];
apply_diffmap_subtract(_Ch, _Diff, []) -> [].

%% @private
%% @doc Assign unused sections or parts of it to the node until the desired size is
%% reached.
-spec apply_diffmap_add(chash_node(), integer(),
                        size_map()) -> size_map().

apply_diffmap_add(Ch, Diff, [{unused, Wt} | T]) ->
    if Wt == Diff -> [{Ch, Wt} | T];
       Wt > Diff -> [{Ch, Diff}, {unused, Wt - Diff} | T];
       Wt < Diff ->
           [{Ch, Wt} | apply_diffmap_add(Ch, Diff - Wt, T)]
    end;
apply_diffmap_add(Ch, Diff, [H | T]) ->
    [H | apply_diffmap_add(Ch, Diff, T)];
apply_diffmap_add(_Ch, _Diff, []) -> [].

%% @private
%% @doc Merge all neighboring sections with the same owner.
-spec combine_neighbors(size_map()) -> size_map().

combine_neighbors([{Ch, Wt1}, {Ch, Wt2} | T]) ->
    combine_neighbors([{Ch, Wt1 + Wt2} | T]);
combine_neighbors([H | T]) ->
    [H | combine_neighbors(T)];
combine_neighbors([]) -> [].

%% @private
%% @doc Asign neighboring unused sections to the successor owner.
-spec collapse_unused_in_size_map(SizeMap ::
                                      size_map()) -> size_map().

collapse_unused_in_size_map([{Ch, Wt1}, {unused, Wt2}
                             | T]) ->
    collapse_unused_in_size_map([{Ch, Wt1 + Wt2} | T]);
collapse_unused_in_size_map([{unused, _}] = L) ->
    L;                                          % Degenerate case only
collapse_unused_in_size_map([H | T]) ->
    [H | collapse_unused_in_size_map(T)];
collapse_unused_in_size_map([]) -> [].

%% @private
%% @doc Convert a size map to an index list.
%% An index list contains pairs of index and owner, where the index is the
%% end of the section owned by the owner, or in the context fo consistent
%% hashing the place of the node.
-spec chash_size_map_to_index_list(SizeMap ::
                                       size_map()) -> index_list().

chash_size_map_to_index_list(SizeMap) ->
    {_Sum, NFs0} = lists:foldl(fun ({Name, Size},
                                    {Sum, List}) ->
                                       {Sum + Size,
                                        [{(Sum + Size) rem hash:max_integer(),
                                          Name}
                                         | List]}
                               end,
                               {0, []}, SizeMap),
    lists:ukeysort(1, NFs0).

%% @private
%% @doc Convert an index list to a size map. A size map contains a mapping of
%% owners to the size of the partition they own.
-spec chash_index_list_to_size_map(IndexList ::
                                       index_list()) -> size_map().

chash_index_list_to_size_map([]) -> [];
chash_index_list_to_size_map([{0, ZeroNode}
                              | IndexList]) ->
    {SizeMap, LastIndex} = lists:foldl(fun ({Index, Name},
                                            {List, PrevIndex}) ->
                                               {[{Name, Index - PrevIndex}
                                                 | List],
                                                Index}
                                       end,
                                       {[], 0}, IndexList),
    lists:reverse([{ZeroNode,
                    hash:max_integer() - LastIndex}
                   | SizeMap]).

%% @private
%% @doc Create a search tree from an index list.
-spec
     chash_index_list_to_index_tree(index_list()) -> index_tree().

chash_index_list_to_index_tree([]) ->
    gb_trees:balance(gb_trees:from_orddict([]));
chash_index_list_to_index_tree(IndexList) ->
    %% Is this still needed with integer?
    %% Should not be necessary because first node is always at 0 and covers
    %% that case.
    % {Index, Name} = lists:last(IndexList),
    %% QuickCheck found a bug ... it really helps to add a catch-all item
    %% at the far "right" of the list ... 42.0 is much greater than 1.0.
    % NFs = case Index < hash:max_integer() of
    %         true -> IndexList ++ [{hash:max_integer(), Name}];
    %         false -> IndexList
    %       end,
    gb_trees:balance(gb_trees:from_orddict(orddict:from_list(IndexList))).

%% @private
%% @doc Query the tree with the key.
-spec chash_gb_next(index() | index_as_int(),
                    index_tree(), chash_node()) -> node_entry().

chash_gb_next(X, {_, GbTree}, ZeroNode)
    when is_integer(X) ->
    chash_gb_next1(X, GbTree, true, ZeroNode);
chash_gb_next(X, T, Z) ->
    chash_gb_next(hash:as_integer(X), T, Z).

%% @private
%% @doc Return the entry associated with the given key according to chash
%% semantics: The responsible node for the key is the one next on the ring.
-spec chash_gb_next1(X :: integer(), term(), boolean(),
                     chash_node()) -> node_entry() | nil.

chash_gb_next1(X, {Key, Val, Left, _Right}, _IsRoot,
               ZeroNode)
    when X < Key ->
    case chash_gb_next1(X, Left, false, ZeroNode) of
      nil -> {Key, Val};
      Res -> Res
    end;
chash_gb_next1(X, {Key, _Val, _Left, Right}, IsRoot,
               ZeroNode)
    when X >= Key ->
    case {chash_gb_next1(X, Right, false, ZeroNode), IsRoot}
        of
      {nil, true} -> {0, ZeroNode};
      {nil, false} -> nil;
      {Res, _} -> Res
    end;
chash_gb_next1(_X, nil, _, _ZeroNode) -> nil.

%% @private
%% @doc Helper function for {@link make_handoff_chash/2}. Merges both index
%%      lists such that they contain the same indices but the ownership of keys
%%      is not changed.
%% @param OriginalIndexList Index list of the original chash structure.
%% @param NextIndexList Index list of the future chash structure.
%% @returns Tupel of original and future index lists.
-spec make_handoff_index_lists(OriginalIndexList ::
                                   index_list(),
                               NextIndexList :: index_list()) -> {index_list(),
                                                                  index_list()}.

make_handoff_index_lists([], []) -> [];
make_handoff_index_lists([{0, OZ} | _] =
                             OriginalIndexList,
                         [{0, NZ} | _] = NextIndexList) ->
    make_handoff_index_lists(OriginalIndexList,
                             NextIndexList, [], [], OZ, NZ).

%% @private
%% @doc Helper for {@link make_handoff_index_list/2}.
%% @param OriginalIndexList Current working list of the original index list.
%% @param NextIndexList Current working list of the future index list.
%% @param OriginalAcc Accumulator list for the merged original list.
%% @param NextAcc Accumulator list for the merged future list.
%% @param OriginalZeroNode Node owning the index `0' on the original list.
%% @param NextZeroNode Node owning the index `0' on the future list.
%% @returns Tuple of merged index lists.
-spec make_handoff_index_lists(OriginalIndexList ::
                                   index_list(),
                               NextIndexList :: index_list(),
                               OriginalAcc :: index_list(),
                               NextAcc :: index_list(),
                               OriginalZeroNode :: chash_node(),
                               NextZeroNode :: chash_node()) -> {index_list(),
                                                                 index_list()}.

make_handoff_index_lists([], [], OriginalAcc, NextAcc,
                         _OriginalZeroNode, _NextZeroNode) ->
    {lists:reverse(OriginalAcc), lists:reverse(NextAcc)};
make_handoff_index_lists([{OI, ON} | OR], [],
                         OriginalAcc, NextAcc, OriginalZeroNode,
                         NextZeroNode) ->
    make_handoff_index_lists(OR, [],
                             [{OI, ON} | OriginalAcc],
                             [{OI, NextZeroNode} | NextAcc], OriginalZeroNode,
                             NextZeroNode);
make_handoff_index_lists([], [{NI, NN} | NR],
                         OriginalAcc, NextAcc, OriginalZeroNode,
                         NextZeroNode) ->
    make_handoff_index_lists([], NR,
                             [{NI, OriginalZeroNode} | OriginalAcc],
                             [{NI, NN} | NextAcc], OriginalZeroNode,
                             NextZeroNode);
make_handoff_index_lists([{OI, ON} | OR],
                         [{NI, NN} | NR], OriginalAcc, NextAcc,
                         OriginalZeroNode, NextZeroNode) ->
    case OI < NI of
      true ->
          make_handoff_index_lists(OR, [{NI, NN} | NR],
                                   [{OI, ON} | OriginalAcc],
                                   [{OI, NN} | NextAcc], OriginalZeroNode,
                                   NextZeroNode);
      false ->
          case OI > NI of
            true ->
                make_handoff_index_lists([{OI, ON} | OR], NR,
                                         [{NI, ON} | OriginalAcc],
                                         [{NI, NN} | NextAcc], OriginalZeroNode,
                                         NextZeroNode);
            false ->
                make_handoff_index_lists(OR, NR,
                                         [{OI, ON} | OriginalAcc],
                                         [{NI, NN} | NextAcc], OriginalZeroNode,
                                         NextZeroNode)
          end
    end.

%% @private
%% @doc Helper for {@link key_to_partition_id/2}.
%% @param Key Integer key.
%% @param IndexList List of Indices without an entry for `0'.
%% @returns Index of the partition the key belongs to.
-spec index_to_partition_id(Index :: integer(),
                            IndexList :: index_list(),
                            CurrentID :: integer()) -> integer() | undefined.

index_to_partition_id(_, [], CurrentID) -> CurrentID;
index_to_partition_id(Index, [{I, _} | R], CurrentID)
    when I /= 0 ->
    case Index =:= I of
      true -> CurrentID;
      false -> index_to_partition_id(Index, R, CurrentID + 1)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

update_test() ->
    Node = old@host,
    NewNode = new@host,
    % Create a fresh ring...
    CHash = {[{1, Node}, {3, Node}, {4, Node}, {6, Node},
              {8, Node}],
             stale},
    GetNthIndex = fun (N, {Nodes, _}) ->
                          {Index, _} = lists:nth(N, Nodes), Index
                  end,
    % Test update...
    FirstIndex = GetNthIndex(1, CHash),
    ThirdIndex = GetNthIndex(3, CHash),
    {[{_, NewNode}, {_, Node}, {_, Node}, {_, Node},
      {_, Node}],
     _} =
        update(FirstIndex, NewNode, CHash),
    {[{_, Node}, {_, Node}, {_, NewNode}, {_, Node},
      {_, Node}],
     _} =
        update(ThirdIndex, NewNode, CHash).

contains_test() ->
    CHash = chash:fresh([{the_node, 1}]),
    ?assertEqual(true, (contains_name(the_node, CHash))),
    ?assertEqual(false,
                 (contains_name(some_other_node, CHash))).

%% Fresh does not use num partitions right now.
% simple_size_test() ->
%     ?assertEqual(8,
%                  (length(chash:nodes(chash:fresh(8, the_node))))).

successors_length_test() ->
    Node = the_node,
    CHash = {[{1, Node}, {2, Node}, {3, Node}, {4, Node},
              {5, Node}, {6, Node}, {7, Node}, {8, Node}],
             stale},
    ?assertEqual(8, (length(chash:successors(0, CHash)))).

inverse_pred_test() ->
    Node = the_node,
    CHash = {[{1, Node}, {2, Node}, {3, Node}, {4, Node},
              {5, Node}, {6, Node}, {7, Node}, {8, Node}],
             stale},
    S = [I
         || {I, _} <- chash:successors(chash:key_of(4), CHash)],
    P = [I
         || {I, _}
                <- chash:predecessors(chash:key_of(4), CHash)],
    ?assertEqual(S, (lists:reverse(P))).

handoff_chash_same_test() ->
    CHash = {[{0, node0}, {1, node1}, {2, node2},
              {3, node3}],
             stale},
    {OHC, NHC} = make_handoff_chash(CHash, CHash),
    ?assertEqual(CHash, OHC),
    ?assertEqual(CHash, NHC).

handoff_chash_simple_test() ->
    OCHash = {[{0, node0}, {2, node1}, {3, node2},
               {6, node3}, {8, node2}],
              stale},
    NCHash = {[{0, node0}, {1, node1}, {2, node2},
               {5, node4}, {6, node2}],
              stale},
    {OHC, NHC} = make_handoff_chash(OCHash, NCHash),
    ExpectedOHC = {[{0, node0}, {1, node1}, {2, node1},
                    {3, node2}, {5, node3}, {6, node3}, {8, node2}],
                   stale},
    ExpectedNHC = {[{0, node0}, {1, node1}, {2, node2},
                    {3, node4}, {5, node4}, {6, node2}, {8, node0}],
                   stale},
    ?assertEqual(OHC, ExpectedOHC),
    ?assertEqual(NHC, ExpectedNHC).

size_map_to_index_list_inverse_test() ->
    SizeMap = make_size_map([{node0, 100}, {node1, 100},
                             {node2, 200}, {node3, 300}]),
    SizeMap2 =
        chash_index_list_to_size_map(chash_size_map_to_index_list(SizeMap)),
    ?assertEqual(SizeMap, SizeMap2).

-endif.

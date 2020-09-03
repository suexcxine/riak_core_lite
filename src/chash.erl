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

%% TODO items:
%%
%%  1. Refactor to use bigints instead of floating point numbers.  The
%%     ?SMALLEST_SIGNIFICANT_FLOAT_SIZE macro below doesn't allow as
%%     much wiggle-room for making really small hashing range
%%     definitions.

-define(SMALLEST_SIGNIFICANT_FLOAT_SIZE, 0.1e-12).

-define(SHA_MAX, 1 bsl 20 * 8).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% -compile(export_all).
-export([make_float_map/1, make_float_map/2,
         sum_map_weights/1, make_tree/1, query_tree/2,
         hash_binary_via_float_map/2,
         hash_binary_via_float_tree/2, pretty_with_integers/2,
         pretty_with_integers/3]).

%% chash API
-export([contains_name/2, fresh/2, index_to_int/1,
         int_to_index/1, lookup/2, lookup_node_entry/2, key_of/1, members/1,
         merge_rings/2, next_index/2, nodes/1, node_size/2,
         offsets/1, predecessors/2, predecessors/3,
         ring_increment/1, size/1,
         successors/2, successors/3, update/3, weights/1]).

%% Owner for a range on the unit interval.  We are agnostic about its
%% type.
-type owner_name() :: term().

%% For this library, a weight is an integer which specifies the
%% capacity of a "owner" relative to other owners.  For example, if
%% owner A with a weight of 10, and if owner B has a weight of 20,
%% then B will be assigned twice as much of the unit interval as A.
-type weight() :: non_neg_integer().

%% A float map subdivides the unit interval, starting at 0.0, to
%% partitions that are assigned to various owners.  The sum of all
%% floats must be exactly 1.0 (or close enough for floating point
%% purposes).
-type float_map() :: [{owner_name(), float()}].

%% A nextfloat_list differs from a float_map in two respects: 1) nextfloat_list
%% contains tuples with the node name in 2nd position, 2) the float() at each
%% position I_n > I_m, for all n, m such that n > m.
%% For example, a nextfloat_list of the float_map example
%% [{node1, 0.25}, {node2, 0.5}, {node3, 0.25}] is
%% [{0.25, node1}, {0.75, node2}, {1.0, node3}].
-type nextfloat_list() :: [{index(), owner_name()}].

%% We can't use gb_trees:tree() because 'nil' (the empty tree) is
%% never valid in our case.  But teaching Dialyzer that is difficult.
-opaque float_tree() :: gb_trees:tree(float(),
                                      owner_name()).

%% Used when "prettying" a float map.
-type owner_int_range() :: {owner_name(),
                            non_neg_integer(), non_neg_integer()}.

-type owner_weight() :: {owner_name(), weight()}.

%% A owner_weight_list is a definition of brick assignments over the
%% unit interval [0.0, 1.0].  The sum of all floats must be 1.0.  For
%% example, [{{br1,nd1}, 0.25}, {{br2,nd1}, 0.5}, {{br3,nd1}, 0.25}].
%% WARN description does not fit types but usage seems to fit.
%% TODO check how this can be streamlined to make working with it easier.
-type owner_weight_list() :: [owner_weight()].

%% The chash type is a pair of a nextfloat_list and the according float tree
%% for an efficient query.
%% When the float tree is not up-to-date with the list it is stale instead.
-type chash() :: {nextfloat_list(),
                  stale | float_tree()}.

-type index() :: float().

-type index_as_int() :: pos_integer().

-type num_partitions() :: pos_integer().

%% A single node is identified by its term and starting index.
-type node_entry() :: {index(), owner_name()}.

-type chash_node() :: owner_name().

%% TODO Is it necessary to export those?
-export_type([float_map/0, float_tree/0]).

%% chash API
-export_type([chash/0, index/0, index_as_int/0]).

%% @doc Create a float map, based on a basic owner weight list.
-spec
     make_float_map(owner_weight_list()) -> float_map().

make_float_map(NewOwnerWeights) ->
    make_float_map([], NewOwnerWeights).

%% @doc Create a float map, based on an older float map and a new weight
%% list.
%%
%% The weights in the new weight list may be different than (or the
%% same as) whatever weights were used to make the older float map.
-spec make_float_map(float_map(),
                     owner_weight_list()) -> float_map().

make_float_map([], NewOwnerWeights) ->
    Sum = add_all_weights(NewOwnerWeights),
    DiffMap = [{Ch, Wt / Sum}
               || {Ch, Wt} <- NewOwnerWeights],
    make_float_map2([{unused, 1.0}], DiffMap,
                    NewOwnerWeights);
make_float_map(OldFloatMap, NewOwnerWeights) ->
    NewSum = add_all_weights(NewOwnerWeights),
    %% Normalize to unit interval
    %% NewOwnerWeights2 = [{Ch, Wt / NewSum} || {Ch, Wt} <- NewOwnerWeights],
    %% Reconstruct old owner weights (will be normalized to unit interval)
    SumOldFloatsDict = lists:foldl(fun ({Ch, Wt},
                                        OrdDict) ->
                                           orddict:update_counter(Ch, Wt,
                                                                  OrdDict)
                                   end,
                                   orddict:new(), OldFloatMap),
    OldOwnerWeights = orddict:to_list(SumOldFloatsDict),
    OldSum = add_all_weights(OldOwnerWeights),
    OldChs = [Ch || {Ch, _} <- OldOwnerWeights],
    NewChs = [Ch || {Ch, _} <- NewOwnerWeights],
    OldChsOnly = OldChs -- NewChs,
    %% Mark any space in by a deleted owner as unused.
    OldFloatMap2 = lists:map(fun ({Ch, Wt} = ChWt) ->
                                     case lists:member(Ch, OldChsOnly) of
                                       true -> {unused, Wt};
                                       false -> ChWt
                                     end
                             end,
                             OldFloatMap),
    %% Create a diff map of changing owners and added owners
    DiffMap = lists:map(fun ({Ch, NewWt}) ->
                                case orddict:find(Ch, SumOldFloatsDict) of
                                  {ok, OldWt} ->
                                      {Ch, NewWt / NewSum - OldWt / OldSum};
                                  error -> {Ch, NewWt / NewSum}
                                end
                        end,
                        NewOwnerWeights),
    make_float_map2(OldFloatMap2, DiffMap, NewOwnerWeights).

%% @doc Not used directly, but can give a developer an idea of how well
%% chash_float_map_to_nextfloat_list will do for a given value of Max.
%%
%% For example:
%% <verbatim>
%%     NewFloatMap = make_float_map([{unused, 1.0}],
%%                                        [{a,100}, {b, 100}, {c, 10}]),
%%     ChashMap = chash_scale_to_int_interval(NewFloatMap, 100),
%%     io:format("QQQ: int int = ~p\n", [ChashIntInterval]),
%% -> [{a,1,47},{b,48,94},{c,94,100}]
%% </verbatim>
%%
%% Interpretation: out of the 100 slots:
%% <ul>
%% <li> 'a' uses the slots 1-47 </li>
%% <li> 'b' uses the slots 48-94 </li>
%% <li> 'c' uses the slots 95-100 </li>
%% </ul>
-spec chash_scale_to_int_interval(NewFloatMap ::
                                      float_map(),
                                  Max :: integer()) -> [owner_int_range()].

chash_scale_to_int_interval(NewFloatMap, Max) ->
    chash_scale_to_int_interval(NewFloatMap, 0, Max).

%% @doc Scales the float map to the int interval [0, Max).
-spec chash_scale_to_int_interval(NewFloatMap ::
                                      float_map(),
                                  Cur :: integer(),
                                  Max :: integer()) -> [{owner_name(),
                                                         integer(), integer()}].

chash_scale_to_int_interval([{Ch, _Wt}], Cur, Max) ->
    [{Ch, Cur, Max}];
chash_scale_to_int_interval([{Ch, Wt} | T], Cur, Max) ->
    Int = trunc(Wt * Max),
    [{Ch, Cur + 1, Cur + Int}
     | chash_scale_to_int_interval(T, Cur + Int, Max)].

%%%%%%%%%%%%%

%% @doc Make a pretty/human-friendly version of a float map that describes
%% integer ranges between 1 and `Scale'.
-spec pretty_with_integers(float_map(),
                           integer()) -> [owner_int_range()].

pretty_with_integers(Map, Scale) ->
    chash_scale_to_int_interval(Map, Scale).

%% @doc Make a pretty/human-friendly version of a float map (based
%% upon a float map created from `OldWeights' and `NewWeights') that
%% describes integer ranges between 1 and `Scale'.
-spec pretty_with_integers(owner_weight_list(),
                           owner_weight_list(),
                           integer()) -> [owner_int_range()].

pretty_with_integers(OldWeights, NewWeights, Scale) ->
    chash_scale_to_int_interval(make_float_map(make_float_map(OldWeights),
                                               NewWeights),
                                Scale).

%% @doc Create a float tree, which is the rapid lookup data structure
%% for consistent hash queries.
-spec make_tree(float_map()) -> float_tree().

make_tree(Map) ->
    chash_nextfloat_list_to_gb_tree(chash_float_map_to_nextfloat_list(Map)).

%% @doc Low-level function for querying a float tree: the (floating
%% point) point within the unit interval.
-spec query_tree(float(), float_tree()) -> {node_entry()}.

query_tree(Val, Tree)
    when is_float(Val), 0.0 =< Val, Val =< 1.0 ->
    chash_gb_next(Val, Tree).

%% @doc Create a human-friendly summary of a float map.
%%
%% The two parts of the summary are: a per-owner total of the unit
%% interval range(s) owned by each owner, and a total sum of all
%% per-owner ranges (which should be 1.0 but is not enforced).
-spec sum_map_weights(float_map()) -> {{per_owner,
                                        float_map()},
                                       {weight_sum, float()}}.

sum_map_weights(Map) ->
    L = sum_map_weights(lists:sort(Map), undefined, 0.0) --
          [{undefined, 0.0}],
    WeightSum = lists:sum([Weight || {_, Weight} <- L]),
    {{per_owner, L}, {weight_sum, WeightSum}}.

sum_map_weights([{SZ, Weight} | T], SZ, SZ_total) ->
    sum_map_weights(T, SZ, SZ_total + Weight);
sum_map_weights([{SZ, Weight} | T], LastSZ,
                LastSZ_total) ->
    [{LastSZ, LastSZ_total} | sum_map_weights(T, SZ,
                                              Weight)];
sum_map_weights([], LastSZ, LastSZ_total) ->
    [{LastSZ, LastSZ_total}].

%% @doc Query a float map with a binary (inefficient).
-spec hash_binary_via_float_map(binary(),
                                float_map()) -> {float(), owner_name()}.

hash_binary_via_float_map(Key, Map) ->
    Tree = make_tree(Map),
    <<Int:(20 * 8)/unsigned>> = crypto:hash(sha, Key),
    Float = Int / (?SHA_MAX),
    query_tree(Float, Tree).

%% @doc Query a float tree with a binary.
-spec hash_binary_via_float_tree(binary(),
                                 float_tree()) -> {float(), owner_name()}.

hash_binary_via_float_tree(Key, Tree) ->
    <<Int:(20 * 8)/unsigned>> = crypto:hash(sha, Key),
    Float = Int / (?SHA_MAX),
    query_tree(Float, Tree).

%% ===================================================================
%% Public API chash
%% ===================================================================

%% @doc Return true if named Node owns any partitions in the ring, else false.
-spec contains_name(Name :: chash_node(),
                    CHash :: chash()) -> boolean().

contains_name(Name, {FloatMap, _}) ->
    lists:keymember(Name, 2, FloatMap).

%% @doc Create a brand new ring.  The size and seednode are specified;
%%      initially all partitions are owned by the seednode.  If NumPartitions
%%      is not much larger than the intended eventual number of
%%       participating nodes, then performance will suffer.
-spec fresh(NumPartitions :: num_partitions(),
            SeedNode :: chash_node()) -> chash().

fresh(_NumPartitions, SeedNode) ->
    %% Not sure what to do with NumPartitions
    %% Currently weight is not considered, set to 100 for every node.
    WeightMap = [{SeedNode, 100}],
    {chash_float_map_to_nextfloat_list(make_float_map(WeightMap)),
     stale}.

%% @doc Converts a given index to its integer representation. Possible loss of
%% precision.
-spec index_to_int(Index :: index()) -> Int ::
                                            integer().

index_to_int(Index) ->
    % WARN possible loss of precision.
    round(Index * (?SHA_MAX)).

%% @doc Converts a given integer representation of an index to its original
%% form. Possible loss of precision.
-spec int_to_index(Int :: integer()) -> Index ::
                                            index().

int_to_index(Int) -> Int / (?SHA_MAX).

%% @doc Find the Node that owns the partition identified by IndexAsInt.
-spec lookup(Index :: index() | index_as_int(),
             CHash :: chash()) -> chash_node().

lookup(Index, CHash) ->
    {_, Node} = lookup_node_entry(Index, CHash),
    Node.

-spec lookup_node_entry(Index :: index() | index_as_int(),
             CHash :: chash()) -> node_entry().

lookup_node_entry(Index, CHash) when is_integer(Index) ->
    lookup_node_entry(int_to_index(Index), CHash);
lookup_node_entry(Index, {NextList, stale}) ->
    %% optimization: also return new chash() with updated tree
    lookup_node_entry(Index,
           {NextList, chash_nextfloat_list_to_gb_tree(NextList)});
lookup_node_entry(Index, FloatTree) ->
    query_tree(Index, FloatTree).

%% @doc Given any term used to name an object, produce that object's key
%%      into the ring.  Two names with the same SHA-1 hash value are
%%      considered the same name.
-spec key_of(ObjectName :: term()) -> index().

key_of(ObjectName) ->
    <<Int:(20 * 8)/unsigned>> = crypto:hash(sha,
                                            term_to_binary(ObjectName)),
    Int / (?SHA_MAX).

%% @doc Return all Nodes that own any partitions in the ring.
-spec members(CHash :: chash()) -> [chash_node()].

members({NextList, _}) ->
    lists:usort([Name || {_, Name} <- NextList]).

%% @doc Return a randomized merge of two rings.
%%      If multiple nodes are actively claiming nodes in the same
%%      time period, churn will occur.  Be prepared to live with it.
%% TODO this is not used in any other module and does not make any sense in the
%% API.
-spec merge_rings(CHashA :: chash(),
                  CHashB :: chash()) -> chash().

merge_rings(CHashA, _CHashB) -> CHashA.

%% @doc Given the integer representation of a chash key,
%%      return the next ring index integer value.
-spec next_index(IntegerKey :: integer(),
                 CHash :: chash()) -> index_as_int().

next_index(_IntegerKey, _Chash) ->
    %% TODO
    %% Since there is no ring structure there is no simple next index. The next
    %% index is determined by the replication strategy.
    %% Used to
    %% - find the integer partition index of a key
    0.

%% @doc Return the entire set of NodeEntries in the ring.
-spec nodes(CHash :: chash()) -> [node_entry()].

nodes(CHash) -> {NextList, _} = CHash, NextList.

%% @doc Returns the distance of the index of the segment belonging to the given
%% key to the index of the next segment
-spec node_size(Index :: index(),
                CHash :: chash()) -> index().

node_size(Index, {NextList, _Tree}) ->
    {Size, _} = lists:foldl(fun ({I, _}, {Start, Done}) ->
                                    case Done of
                                      true -> {Start, true};
                                      false ->
                                          case (Index >= Start) and (Index < I)
                                              of
                                            true -> {I - Start, true};
                                            false -> {I, false}
                                          end
                                    end
                            end,
                            {0.0, false}, NextList),
    Size.

%% @doc Return a list of section sizes as the index type.
-spec offsets(CHash :: chash()) -> [index()].

offsets({NextList, _}) ->
    {Offsets, _} = lists:foldl(fun ({I, _N}, {O, C}) ->
                                       {[I - C | O], I}
                               end,
                               {[], 0.0}, NextList),
    lists:reverse(Offsets).

%% @doc Given an object key, return all NodeEntries in reverse order
%%      starting at Index.
-spec predecessors(Index :: index() | index_as_int(),
                   CHash :: chash()) -> [node_entry()].

predecessors(_Index, _CHash) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    %% Used to
    %% - find first predecessor when scheduling resize in riak_core_claimant
    %% - find repair pairs in riak_core_vnode_manager
    [].

%% @doc Given an object key, return the next N NodeEntries in reverse order
%%      starting at Index.
-spec predecessors(Index :: index() | index_as_int(),
                   CHash :: chash(), N :: integer()) -> [node_entry()].

predecessors(_Index, _CHash, _N) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    %% Used to
    %% - find first predecessor when scheduling resize in riak_core_claimant
    %% - find repair pairs in riak_core_vnode_manager
    [].

%% @doc Return increment between ring indexes given
%% the number of ring partitions.
-spec ring_increment(NumPartitions ::
                         pos_integer()) -> pos_integer().

ring_increment(_NumPartitions) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    %% Used to
    %% - find Indices in chash_bin
    %% - determine partition ID in riak_core_ring_util
    %% - To help compute the future ring in riak_core_ring
    %% - Various test scenarios
    0.

%% @doc Return the number of partitions in the ring.
-spec size(CHash :: chash()) -> integer().

size({NextList, _, _}) -> lists:size(NextList).

%% @doc Given an object key, return all NodeEntries in order starting at Index.
-spec successors(Index :: index(),
                 CHash :: chash()) -> [node_entry()].

successors(_Index, _CHash) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    %% Used as preflist and to find pairs involved in a repair operation
    [].

%% @doc Given an object key, return the next N NodeEntries in order
%%      starting at Index.
-spec successors(Index :: index(), CHash :: chash(),
                 N :: integer()) -> [node_entry()].

successors(_Index, _CHash, _N) ->
    %% TODO
    %% Since there is no ring structure there are no predecessors. Depending on
    %% where and how this is used adaptations need to be done in the affected
    %% modules.
    [].

%% @doc Make the partition beginning at IndexAsInt owned by Name'd node.
-spec update(IndexAsInt :: index_as_int(),
             Name :: chash_node(), CHash :: chash()) -> chash().

%% Used when resizing the ring, renaming a node, and transferring a node to a
%% partition. How to abstract from that?
update(_IndexAsInt, _Name, _CHash) ->
    {}. %% TODO

%% @doc Retrieves the current weight list of the chash.
-spec weights(CHash :: chash()) -> [weight()].

weights({_, _, Weights}) -> Weights.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
%% Chooses a random weight out of two.
% -spec random_weight(WeightA :: weight(),
%                     WeightB :: weight()) -> weight().

% random_weight(WeightA, WeightB) ->
%     lists:nth(rand:uniform(2), [WeightA, WeightB]).

%% @private
%% Assigns gaps to nodes and merges neighbouring sections with the same owner.
-spec make_float_map2(OldFloatMap :: float_map(),
                      DiffMap :: float_map(),
                      NewOwnerWeights :: owner_weight_list()) -> float_map().

make_float_map2(OldFloatMap, DiffMap,
                _NewOwnerWeights) ->
    FloatMap = apply_diffmap(DiffMap, OldFloatMap),
    XX =
        combine_neighbors(collapse_unused_in_float_map(FloatMap)),
    XX.

%% @private
%%
-spec apply_diffmap(DiffMap :: float_map(),
                    FloatMap :: float_map()) -> float_map().

apply_diffmap(DiffMap, FloatMap) ->
    SubtractDiff = [{Ch, abs(Diff)}
                    || {Ch, Diff} <- DiffMap, Diff < 0],
    AddDiff = [D || {_Ch, Diff} = D <- DiffMap, Diff > 0],
    TmpFloatMap = iter_diffmap_subtract(SubtractDiff,
                                        FloatMap),
    iter_diffmap_add(AddDiff, TmpFloatMap).

%% @private
%% Sums the weights of all nodes.
-spec add_all_weights(OwnerWeights ::
                          owner_weight_list()) -> float().

add_all_weights(OwnerWeights) ->
    lists:foldl(fun ({_Ch, Weight}, Sum) -> Sum + Weight
                end,
                0.0, OwnerWeights).

%% @private
%% Takes sections or part of them away from each node such that they own the
%% desired size.
-spec iter_diffmap_subtract(DiffMap :: float_map(),
                            FloatMap :: float_map()) -> float_map().

iter_diffmap_subtract([{Ch, Diff} | T], FloatMap) ->
    iter_diffmap_subtract(T,
                          apply_diffmap_subtract(Ch, Diff, FloatMap));
iter_diffmap_subtract([], FloatMap) -> FloatMap.

%% @private
%% Assigns sections or part of them to each node such that they own the desired
%% size.
-spec iter_diffmap_add(DiffMap :: float_map(),
                       FloatMap :: float_map()) -> float_map().

iter_diffmap_add([{Ch, Diff} | T], FloatMap) ->
    iter_diffmap_add(T,
                     apply_diffmap_add(Ch, Diff, FloatMap));
iter_diffmap_add([], FloatMap) -> FloatMap.

%% @private
%% Substracts the size difference from sections owned by the node until the
%% desired size is reached.
-spec apply_diffmap_subtract(node(), float(),
                             float_map()) -> float_map().

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
%% Assigns unused sections or parts of it to the node until the desired size is
%% reached.
-spec apply_diffmap_add(node(), float(),
                        float_map()) -> float_map().

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
%% Mergers all neighboring sections with the same owner.
-spec combine_neighbors(float_map()) -> float_map().

combine_neighbors([{Ch, Wt1}, {Ch, Wt2} | T]) ->
    combine_neighbors([{Ch, Wt1 + Wt2} | T]);
combine_neighbors([H | T]) ->
    [H | combine_neighbors(T)];
combine_neighbors([]) -> [].

%% @private
%% Asigns neighboring unused sections to the successor owner.
-spec collapse_unused_in_float_map(FloatMap ::
                                       float_map()) -> float_map().

collapse_unused_in_float_map([{Ch, Wt1}, {unused, Wt2}
                              | T]) ->
    collapse_unused_in_float_map([{Ch, Wt1 + Wt2} | T]);
collapse_unused_in_float_map([{unused, _}] = L) ->
    L;                                          % Degenerate case only
collapse_unused_in_float_map([H | T]) ->
    [H | collapse_unused_in_float_map(T)];
collapse_unused_in_float_map([]) -> [].

%% @private
%% Converts a float map to a nextfloat list.
%% A nextfloat list contains pairs of index and owner, where the index is the
%% end of the section owned by the owner.
-spec chash_float_map_to_nextfloat_list(FloatMap ::
                                            float_map()) -> nextfloat_list().

chash_float_map_to_nextfloat_list(FloatMap)
    when length(FloatMap) > 0 ->
    %% QuickCheck found a bug ... need to weed out stuff smaller than
    %% ?SMALLEST_SIGNIFICANT_FLOAT_SIZE here.
    FM1 = [P
           || {_X, Y} = P <- FloatMap,
              Y > (?SMALLEST_SIGNIFICANT_FLOAT_SIZE)],
    {_Sum, NFs0} = lists:foldl(fun ({Name, Amount},
                                    {Sum, List}) ->
                                       {Sum + Amount,
                                        [{Sum + Amount, Name} | List]}
                               end,
                               {0, []}, FM1),
    lists:reverse(NFs0).

%% @private
%% Converts a nextfloat list to a float map.
% -spec nextfloat_list_to_float_map(NextFloatList ::
%                                       nextfloat_list()) -> float_map().

% nextfloat_list_to_float_map(NextFloatList) ->
%     {_, L} = lists:foldl(fun ({Index, Name},
%                               {Start, List}) ->
%                                  {Index, [{Name, Index - Start} | List]}
%                          end,
%                          {0.0, []}, NextFloatList),
%     L.

%% @private
%% Creates a search tree from a nextfloat list.
-spec
     chash_nextfloat_list_to_gb_tree(nextfloat_list()) -> gb_trees:tree().

chash_nextfloat_list_to_gb_tree([]) ->
    gb_trees:balance(gb_trees:from_orddict([]));
chash_nextfloat_list_to_gb_tree(NextFloatList) ->
    {_FloatPos, Name} = lists:last(NextFloatList),
    %% QuickCheck found a bug ... it really helps to add a catch-all item
    %% at the far "right" of the list ... 42.0 is much greater than 1.0.
    NFs = NextFloatList ++ [{42.0, Name}],
    gb_trees:balance(gb_trees:from_orddict(orddict:from_list(NFs))).

%% @private
%% Queries the tree with the key.
-spec chash_gb_next(float(), float_tree()) -> {node_entry()}.

chash_gb_next(X, {_, GbTree}) ->
    chash_gb_next1(X, GbTree).

%% @private
%% Queries a single node of a gb tree.
-spec chash_gb_next1(X :: float(), term()) -> {float(),
                                               owner_name()}.

chash_gb_next1(X, {Key, Val, Left, _Right})
    when X < Key ->
    case chash_gb_next1(X, Left) of
      nil -> {Key, Val};
      Res -> Res
    end;
chash_gb_next1(X, {Key, _Val, _Left, Right})
    when X >= Key ->
    chash_gb_next1(X, Right);
chash_gb_next1(_X, nil) -> nil.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

update_test() ->
    Node = old@host,
    NewNode = new@host,
    % Create a fresh ring...
    CHash = chash:fresh(5, Node),
    GetNthIndex = fun (N, {_, Nodes}) ->
                          {Index, _} = lists:nth(N, Nodes), Index
                  end,
    % Test update...
    FirstIndex = GetNthIndex(1, CHash),
    ThirdIndex = GetNthIndex(3, CHash),
    {5,
     [{_, NewNode}, {_, Node}, {_, Node}, {_, Node},
      {_, Node}, {_, Node}]} =
        update(FirstIndex, NewNode, CHash),
    {5,
     [{_, Node}, {_, Node}, {_, NewNode}, {_, Node},
      {_, Node}, {_, Node}]} =
        update(ThirdIndex, NewNode, CHash).

contains_test() ->
    CHash = chash:fresh(8, the_node),
    ?assertEqual(true, (contains_name(the_node, CHash))),
    ?assertEqual(false,
                 (contains_name(some_other_node, CHash))).

simple_size_test() ->
    ?assertEqual(8,
                 (length(chash:nodes(chash:fresh(8, the_node))))).

successors_length_test() ->
    ?assertEqual(8,
                 (length(chash:successors(chash:key_of(0),
                                          chash:fresh(8, the_node))))).

inverse_pred_test() ->
    CHash = chash:fresh(8, the_node),
    S = [I
         || {I, _} <- chash:successors(chash:key_of(4), CHash)],
    P = [I
         || {I, _}
                <- chash:predecessors(chash:key_of(4), CHash)],
    ?assertEqual(S, (lists:reverse(P))).

merge_test() ->
    CHashA = chash:fresh(8, node_one),
    CHashB = chash:update(0, node_one,
                          chash:fresh(8, node_two)),
    CHash = chash:merge_rings(CHashA, CHashB),
    ?assertEqual(node_one, (chash:lookup(0, CHash))).

-endif.

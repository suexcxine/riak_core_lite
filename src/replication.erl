%% @doc Library module for different replication algorithms.
%% In the context of this module a replication algorithm does not handle the
%% actual storage of the data but rather constructs an ordered list of nodes
%% the replications should be stored on (preflist).
%% The algorithm used is determined by the configuration
%% 'riak_core:replication'.
-module(replication).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

-export([replicate/2]).

-type index() :: chash:index_as_int().

-type chash() :: chash:chash().

-type node_entry() :: chash:node_entry().

-spec replicate(Key :: index(),
                CHash :: chash()) -> {[node_entry()], chash()}.

%% @doc Constructs the preference list according to the algorithm set in the
%% riak_core:replication configuration key or random by defualt.
%% -random: draw random bins until enough are drawn
%% -rotation: rotate the key around the ring with the step length depending on
%% the segment lengths in the ring
%% -incremental: rotate the key around the ring with the step lengths depending
%% on the segment the key currently belongs to
replicate(Key, CHash) ->
    replicate(application:getenv(riak_core, replication,
                                 random),
              Key, CHash).

%% @doc Constructs the preference list according to the given algorithm:
%% -random: draw random bins until enough are drawn
%% -rotation: rotate the key around the ring with the step length depending on
%% the segment lengths in the ring
%% -incremental: rotate the key around the ring with the step lengths depending
%% on the segment the key currently belongs to
-spec replicate(Method :: random | rotation |
                          incremental,
                Key :: index(), CHash :: chash()) -> {[node_entry()],
                                                      chash()}.

replicate(random, Key, CHash) -> random(Key, CHash);
replicate(rotation, Key, CHash) -> rotation(Key, CHash);
replicate(incremental, Key, CHash) ->
    incremental(Key, CHash).

%% @private
%% Constructs the preference list for the given key via the random
%% algorithm.
-spec random(index(), chash()) -> {[node_entry()],
                                   chash()}.

random(Key, CHash) ->
    rand:seed(exsss, hash:as_integer(Key)),
    {NodeEntry, CHash2} = chash:lookup_node_entry(Key,
                                                  CHash),
    {PrefList, CHash3} = random(CHash2,
                                length(chash:members(CHash2)), [NodeEntry]),
    {lists:reverse(PrefList), CHash3}.

%% @private
%% Constructs the preference list for the given key via the rotation
%% algorithm.
-spec random(chash(), pos_integer(),
             [node_entry()]) -> {[node_entry()], chash()}.

random(CHash, N, PrefList) ->
    case length(PrefList) >= N of
      true -> {PrefList, CHash};
      false ->
          {Node, CHash2} =
              chash:lookup_node_entry(hash:as_integer(rand:uniform()),
                                      CHash),
          NPref = case lists:member(Node, PrefList) of
                    true -> PrefList;
                    false -> [Node | PrefList]
                  end,
          random(CHash2, N, NPref)
    end.

%% @private
%% Constructs the preference list for the given key via the rotation
%% algorithm.
-spec rotation(index(), chash()) -> {[node_entry()],
                                     chash()}.

rotation(Key, CHash) ->
    {Node, CHash2} = chash:lookup_node_entry(Key, CHash),
    {PrefList, CHash3} = rotation(Key, CHash2,
                                  length(chash:members(CHash2)),
                                  chash:offsets(CHash2), [], [Node], 0),
    {lists:reverse(PrefList), CHash3}.

%% @private
%% Constructs the preference list for the given key via the rotation
%% algorithm.
-spec rotation(index(), chash(), pos_integer(),
               [index()], [index()], [node_entry()],
               non_neg_integer()) -> {[node_entry()], chash()}.

rotation(Key, CHash, N, [], NextOffsets, PrefList, I) ->
    rotation(Key, CHash, N, lists:reverse(NextOffsets), [],
             PrefList, I);
rotation(Key, CHash, N, Offsets, NextOffsets, PrefList,
         I) ->
    case length(PrefList) >= N of
      true -> {PrefList, CHash};
      false ->
          [Offset | Rest] = Offsets,
          %% WARN potential rounding errors
          %% need to look into a more sophisticated creation of subsections
          Step = math:round(Offset / math:pow(2, I)),
          {{NKey, NPref}, NCHash} = step(Key, CHash, Step,
                                         PrefList),
          {{NNKey, NNPref}, NNCHash} = rotate(NKey, NCHash,
                                              Step * 2, NPref, I),
          NNNKey = increment(NNKey, Offset),
          rotation(NNNKey, NNCHash, N, Rest,
                   [Offset | NextOffsets], NNPref, I + 1)
    end.

%% @private
%% Constructs the preference list for the given key via the incremental
%% algorithm.
-spec incremental(index(), chash()) -> {[node_entry()],
                                        chash()}.

incremental(Key, CHash) ->
    {Node, CHash2} = chash:lookup_node_entry(Key, CHash),
    incremental(Key, CHash2, length(chash:members(CHash2)),
                [Node]).

%% @private
%% Constructs the preference list for the given key via the incremental
%% algorithm.
-spec incremental(index(), chash(), pos_integer(),
                  [node_entry()]) -> {[node_entry()], chash()}.

incremental(Key, CHash, N, PrefList) ->
    case length(PrefList) >= N of
      true -> {PrefList, CHash};
      false ->
          {{NKey, NPref}, CHash2} = step(Key, CHash,
                                         chash:node_size(Key, CHash), PrefList),
          incremental(NKey, CHash2, N, NPref)
    end.

%% =============================================================================
%% PRIVATE FUNCTIONS
%% =============================================================================

%% @private
%% Moves the key by the offset around the ring.
-spec increment(index(), index()) -> index().

increment(Key, Offset) ->
    (Key + Offset) rem hash:max_integer().

%% @private
%% Moves the key by offset and adds the owning node to the preference list.
-spec step(index(), chash(), index(),
           [node_entry()]) -> {{index(), [node_entry()]}, chash()}.

step(Key, CHash, Offset, PrefList) ->
    NKey = increment(Key, Offset),
    {Node, CHash2} = chash:lookup_node_entry(NKey, CHash),
    NPref = case lists:member(Node, PrefList) of
              true -> PrefList;
              false -> [Node | PrefList]
            end,
    {{NKey, NPref}, CHash2}.

%% @private
%% Rotates the key for one section
-spec rotate(index(), chash(), index(), [node_entry()],
             non_neg_integer()) -> {{index(), [node_entry()]},
                                    chash()}.

rotate(Key, CHash, Offset, PrefList, I) ->
    C = lists:seq(1,
                  max(0, 1 bsl (I - 1) - 1)), % 2^(I-1)-1 steps
    lists:foldl(fun (_, {{AKey, APref}, ACHash}) ->
                        step(AKey, ACHash, Offset, APref)
                end,
                {{Key, PrefList}, CHash}, C).

%% =============================================================================
%% EUNIT TESTS
%% =============================================================================

-ifdef(TEST).

-define(TEST_KEY, 3 bsl 157).

test_chash() ->
    W0 = [{node0, 100}],
    W1 = [{node0, 100}, {node1, 100}],
    W2 = [{node0, 100}, {node1, 100}, {node2, 100}],
    W3 = [{node0, 100}, {node1, 100}, {node2, 100},
          {node3, 100}],
    W4 = [{node0, 100}, {node1, 100}, {node2, 100},
          {node3, 150}],
    F = lists:foldl(fun (WM, FM) ->
                            chash:make_float_map(FM, WM)
                    end,
                    [], [W0, W1, W2, W3, W4]),
    {F, stale, W4}.

is_deterministic(Mode) ->
    CHash = test_chash(),
    {PrefList, Chash2} = replicate(Mode, ?TEST_KEY, CHash),
    lists:all(fun (_I) ->
                      {PrefList2, _} = replicate(Mode, ?TEST_KEY, CHash2),
                      PrefList2 == PrefList
              end,
              lists:seq(1, 100)).

is_complete(Mode) ->
    CHash = test_chash(),
    N = 4,
    {PrefList, _} = replicate(Mode, ?TEST_KEY, CHash),
    length(PrefList) == N.

is_unique(Mode) ->
    CHash = test_chash(),
    {PrefList, _} = replicate(Mode, ?TEST_KEY, CHash),
    PrefNodes = [N || {I, N} <- PrefList],
    length(PrefList) ==
      sets:size(sets:from_list(PrefNodes)).

determinism_random_test() ->
    ?assert((is_deterministic(random))).

determinism_rotation_test() ->
    ?assert((is_deterministic(rotation))).

determinism_incremental_test() ->
    ?assert((is_deterministic(incremental))).

completeness_random_test() ->
    ?assert((is_complete(random))).

completeness_rotation_test() ->
    ?assert((is_complete(rotation))).

completeness_incremental_test() ->
    ?assert((is_complete(incremental))).

uniqueness_random_test() ->
    ?assert((is_unique(random))).

uniqueness_rotation_test() ->
    ?assert((is_unique(rotation))).

uniqueness_incremental_test() ->
    ?assert((is_unique(incremental))).

-endif.

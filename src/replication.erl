%% @doc Implementation

-module(replication).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

-export([replicate/3]).

%% @doc Constructs the preference list according to the given algorithm:
%% -random: draw random bins until enough are drawn
%% -rotation: rotate the key around the ring with the step length depending on
%% the segment lengths in the ring
%% -incremental: rotate the key around the ring with the step lengths depending
%% on the segment the key currently belongs to
-spec replicate(Method :: random | rotation |
                          incremental,
                Key :: chash:index(),
                CHash :: chash:chash()) -> chash:preflist().

replicate(random, Key, CHash) -> random(Key, CHash);
replicate(rotation, Key, CHash) -> rotation(Key, CHash);
replicate(incremental, Key, CHash) ->
    incremental(Key, CHash);
% default:
replicate(_, Key, CHash) -> random(Key, CHash).

random(Key, CHash) ->
    rand:seed(exsss,
              chash:index_to_int(Key)), % Not sure if this will actually work.
    lists:reverse(random(CHash,
                         length(chash:members(CHash)),
                         [chash:lookup(Key, CHash)])).

random(CHash, N, PrefList) ->
    case length(PrefList) >= N of
      true -> PrefList;
      false ->
          Node = chash:lookup(rand:uniform(), CHash),
          NPref = case lists:member(Node, PrefList) of
                    true -> PrefList;
                    false -> [Node | PrefList]
                  end,
          random(CHash, N, NPref)
    end.

rotation(Key, CHash) ->
    lists:reverse(rotation(Key, CHash,
                           length(chash:members(CHash)), chash:offsets(CHash),
                           [], [chash:lookup(Key, CHash)], 0)).

rotation(Key, CHash, N, [], NextOffsets, PrefList, I) ->
    rotation(Key, CHash, N, lists:reverse(NextOffsets), [],
             PrefList, I);
rotation(Key, CHash, N, Offsets, NextOffsets, PrefList,
         I) ->
    case length(PrefList) >= N of
      true -> PrefList;
      false ->
          [Offset | Rest] = Offsets,
          Step = Offset / math:pow(2, I),
          {NKey, NPref} = step(Key, CHash, Step, PrefList),
          {NNKey, NNPref} = rotate(NKey, CHash, Step * 2, NPref,
                                   I),
          NNNKey = increment(NNKey, Offset),
          rotation(NNNKey, CHash, N, Rest, [Offset | NextOffsets],
                   NNPref, I + 1)
    end.

rotate(Key, CHash, Offset, PrefList, I) ->
    C = lists:seq(1,
                  max(0, 1 bsl (I - 1) - 1)), % 2^(I-1)-1 steps
    lists:foldl(fun (_, {AKey, APref}) ->
                        step(AKey, CHash, Offset, APref)
                end,
                {Key, PrefList}, C).

incremental(Key, CHash) ->
    incremental(Key, CHash, length(chash:members(CHash)),
                [chash:lookup(Key, CHash)]).

incremental(Key, CHash, N, PrefList) ->
    case length(PrefList) >= N of
      true -> PrefList;
      false ->
          {NKey, NPref} = step(Key, CHash,
                               chash:node_size(Key, CHash), PrefList),
          incremental(NKey, CHash, N, NPref)
    end.

%% =============================================================================
%% PRIVATE FUNCTIONS
%% =============================================================================

%% @private
increment(Key, Offset) ->
    % WARN Only works with key on unit interval
    % TODO Abstract to any value range of Key
    case Key + Offset >= 1.0 of
      true -> Key + Offset - 1.0;
      false -> Key + Offset
    end.

step(Key, CHash, Offset, PrefList) ->
    NKey = increment(Key, Offset),
    Node = chash:lookup(NKey, CHash),
    NPref = case lists:member(Node, PrefList) of
              true -> PrefList;
              false -> [Node | PrefList]
            end,
    {NKey, NPref}.

%% =============================================================================
%% EUNIT TESTS
%% =============================================================================

-ifdef(TEST).

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
    {F, {stale, {}}, W4}.

is_deterministic(Mode) ->
    Key = 0.345,
    CHash = test_chash(),
    PrefList = replicate(Mode, Key, CHash),
    lists:all(fun (_I) ->
                      replicate(Mode, Key, CHash) == PrefList
              end,
              lists:seq(1, 100)).

is_complete(Mode) ->
    Key = 0.345,
    CHash = test_chash(),
    N = 4,
    length(replicate(Mode, Key, CHash)) == N.

is_unique(Mode) ->
    Key = 0.345,
    CHash = test_chash(),
    PrefList = replicate(Mode, Key, CHash),
    length(PrefList) == sets:size(sets:from_list(PrefList)).

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

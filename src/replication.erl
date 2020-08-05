-module(replication).

-export([replicate/3]).

-spec replicate(Method :: random | rotation |
                          incremental,
                Key :: chash:index(),
                CHash :: chash:chash()) -> chash:preflist().

replicate(random, Key, CHash) -> random(Key, CHash);
replicate(rotation, Key, CHash) -> rotation(Key, CHash);
replicate(incremental, Key, CHash) ->
    incremental(Key, CHash).

random(Key, CHash) ->
    crypto:rand_seed_alg(ctypto_aes,
                         Key), % Not sure if this will actually work.
    lists:reverse(random(CHash,
                         lists:size(chash:members(CHash)),
                         [chash:lookup(Key)])).

rotation(Key, CHash) ->
    rotation(Key, CHash, lists:size(chash:members(CHash)),
             offsets(CHash), [], []).

incremental(Key, CHash) -> [].

random(CHash, N, PrefList) ->
    case lists:size(PrefList) >= N of
      true -> PrefList;
      false ->
          Node = chash:lookup(rand:uniform()),
          NPref = case lists:member(PrefList, Node) of
                    true -> PrefList;
                    false -> [Node | PrefList]
                  end,
          random(CHash, N, NPref)
    end.

rotation(Key, CHash, N, [], NextOffsets, PrefList) ->
    rotation(Key, CHash, N, lists:reverse(NextOffsets), [],
             PrefList);
rotation(Key, CHash, N, Offsets, NextOffsets,
         PrefList) ->
    Node = chash:lookup(Key),
    NPref = case lists:member(PrefList, Node) of
              true -> PrefList;
              false -> [Node | PrefList]
            end,
    case lists:size(NPref) >= N of
      true -> NPref;
      false ->
          [Offset | Rest] = Offsets,
          NKey = case Key + Offset >= 1.0 of
                   true -> Key + Offset - 1.0;
                   false -> Key + Offset
                 end,
          rotation(NKey, CHash, N, Rest,
                   [Offset / 2, Offset / 2 | NextOffsets], NPref)
    end.

offsets(CHash) ->
    [chash:node_size(CHash, NodeEntry)
     || NodeEntry
            <- chash:nodes(CHash)].    %% Using node_size instead of computing them from node entries is inefficient for rslicing

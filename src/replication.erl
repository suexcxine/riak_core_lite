-module(replication).

-export([replicate/3]).

-spec replicate(Method :: random | rotation | incremental,
    Key :: chash:index(), CHash :: chash:chash()) -> chash:preflist().

replicate(random, Key, CHash) ->
    random(Key, CHash);
replicate(rotation, Key, CHash) ->
    rotation(Key, CHash);
replicate(incremental, Key, CHash) ->
    incremental(Key, CHash).

random(Key, CHash) ->
    crypto:rand_seed_alg(ctypto_aes, Key), % Not sure if this will actually work.
    lists:reverse(random(CHash, lists:size(chash:members(CHash)), [chash:lookup(Key)])).

rotation(Key, CHash) ->
    [].

incremental(Key, CHash) ->
    [].



random(CHash, N, PrefList) ->
    case lists:size(PrefList) >= N of
        true ->
            PrefList;
        false ->
            Node = chash:lookup(rand:uniform()),
            NPref =
                case lists:member(PrefList, Node) of
                    true ->
                        PrefList;
                    false ->
                        [Node | PrefList]
                end,
            random(CHash, N, NPref)
    end.
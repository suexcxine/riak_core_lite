%% -------------------------------------------------------------------
%%
%% riak_core: Core Active Preference Lists
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
%% Get active preference list - preference list with secondary nodes
%% substituted.
%% -------------------------------------------------------------------
-module(riak_core_apl).

-export([active_owners/1, active_owners/2, get_apl/3,
         get_apl/4, get_apl_ann/2, get_apl_ann/3, get_apl_ann/4,
         get_apl_ann_with_pnum/1, get_primary_apl/3,
         get_primary_apl/4, get_primary_apl_chbin/4, first_up/2,
         offline_owners/1, offline_owners/2]).

-export_type([preflist/0, preflist_ann/0,
              preflist_with_pnum_ann/0]).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

-type bucket() :: binary() | {binary(), binary()}.

-type index() :: chash:index_as_int().

-type n_val() :: non_neg_integer().

-type ring() :: riak_core_ring:riak_core_ring().

-type preflist() :: [{index(), node()}].

-type preflist_ann() :: [{{index(), node()},
                          primary | fallback}].

%% @type preflist_with_pnum_ann().
%% Annotated preflist where the partition value is an id/number
%% (0 to ring_size-1) instead of a hash.
-type
     preflist_with_pnum_ann() :: [{{riak_core_ring:partition_id(),
                                    node()},
                                   primary | fallback}].

-type chashbin() :: term().

-type docidx() :: chash:index().

%% @doc Return preflist of all active primary nodes (with no
%%      substituion of fallbacks).  Used to simulate a
%%      preflist with N=ring_size.
-spec active_owners(atom()) -> preflist_ann().

active_owners(Service) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    active_owners(Ring,
                  riak_core_node_watcher:nodes(Service)).

-spec active_owners(ring(), [node()]) -> preflist_ann().

active_owners(Ring, UpNodes) ->
    UpNodes1 = UpNodes,
    Primaries = riak_core_ring:all_owners(Ring),
    {Up, _Pangs} = check_up(Primaries, UpNodes1, [], []),
    Up.

%% @doc Get the active preflist taking account of which nodes are up.
-spec get_apl(docidx(), n_val(), atom()) -> preflist().

get_apl(DocIdx, N, Service) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    get_apl_chbin(DocIdx, N, CHBin,
                  riak_core_node_watcher:nodes(Service)).

%% @doc Get the active preflist taking account of which nodes are up
%%      for a given chash/upnodes list.
-spec get_apl_chbin(docidx(), n_val(),
                    chashbin:chashbin(), [node()]) -> preflist().

get_apl_chbin(DocIdx, N, CHBin, UpNodes) ->
    [{Partition, Node}
     || {{Partition, Node}, _Type}
            <- get_apl_ann_chbin(DocIdx, N, CHBin, UpNodes)].

%% @doc Get the active preflist taking account of which nodes are up
%%      for a given ring/upnodes list.
-spec get_apl(docidx(), n_val(), ring(),
              [node()]) -> preflist().

get_apl(DocIdx, N, Ring, UpNodes) ->
    [{Partition, Node}
     || {{Partition, Node}, _Type}
            <- get_apl_ann(DocIdx, N, Ring, UpNodes)].

%% @doc Get the active preflist taking account of which nodes are up for a given
%%      chash/upnodes list and annotate each node with type of primary/fallback.
get_apl_ann(DocIdx, N, UpNodes) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    get_apl_ann_chbin(DocIdx, N, CHBin, UpNodes).

%% @doc Get the active preflist taking account of which nodes are up
%%      for a given ring/upnodes list and annotate each node with type of
%%      primary/fallback.
-spec get_apl_ann(binary(), n_val(), ring(),
                  [node()]) -> preflist_ann().

get_apl_ann(DocIdx, N, Ring, UpNodes) ->
    UpNodes1 = UpNodes,
    Preflist = riak_core_ring:preflist(DocIdx, Ring),
    {Primaries, Fallbacks} = lists:split(N, Preflist),
    {Up, Pangs} = check_up(Primaries, UpNodes1, [], []),
    Up ++ find_fallbacks(Pangs, Fallbacks, UpNodes1, []).

%% @doc Get the active preflist for a given {bucket, key} and list of nodes
%%      and annotate each node with type of primary/fallback.
-spec get_apl_ann(bucket(), [node()]) -> preflist_ann().

get_apl_ann({Bucket, Key}, UpNodes) ->
    {ok, NVal} = application:get_env(riak_core,
                                     target_n_val),
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    get_apl_ann(DocIdx, NVal, UpNodes).

%% @doc Get the active preflist taking account of which nodes are up
%%      for a given {bucket, key} and annotate each node with type of
%%      primary/fallback
-spec
     get_apl_ann_with_pnum(bucket()) -> preflist_with_pnum_ann().

get_apl_ann_with_pnum(BKey) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    UpNodes = riak_core_ring:all_members(Ring),
    Apl = get_apl_ann(BKey, UpNodes),
    Size = riak_core_ring:num_partitions(Ring),
    apl_with_partition_nums(Apl, Size).

%% @doc Get the active preflist taking account of which nodes are up
%%      for a given chash/upnodes list and annotate each node with type of
%%      primary/fallback.
%%      Does not work directly with the chashbin and is therefor slow.
%%      The implementation uses get_apl_ann. THis function is only left to
%%      assure API compatibility.
-spec get_apl_ann_chbin(binary(), n_val(), chashbin(),
                        [node()]) -> preflist_ann().

get_apl_ann_chbin(DocIdx, N, _CHBin, UpNodes) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    get_apl_ann(DocIdx, N, Ring, UpNodes).

%% @doc Same as get_apl, but returns only the primaries.
-spec get_primary_apl(binary(), n_val(),
                      atom()) -> preflist_ann().

get_primary_apl(DocIdx, N, Service) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    get_primary_apl(DocIdx, N, Ring,
                    riak_core_node_watcher:nodes(Service)).

%% @doc Same as get_apl, but returns only the primaries.
%%      Does not work directly with the chashbin and is therefor slow.
%%      The implementation uses get_primary_apl. THis function is only left to
%%      assure API compatibility.
-spec get_primary_apl_chbin(binary(), n_val(),
                            chashbin(), [node()]) -> preflist_ann().

get_primary_apl_chbin(DocIdx, N, _CHBin, UpNodes) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    get_primary_apl(DocIdx, N, Ring, UpNodes).

%% @doc Same as get_apl, but returns only the primaries.
-spec get_primary_apl(binary(), n_val(), ring(),
                      [node()]) -> preflist_ann().

get_primary_apl(DocIdx, N, Ring, UpNodes) ->
    UpNodes1 = UpNodes,
    Preflist = riak_core_ring:preflist(DocIdx, Ring),
    {Primaries, _} = lists:split(N, Preflist),
    {Up, _} = check_up(Primaries, UpNodes1, [], []),
    Up.

%% @doc Return the first entry that is up in the preflist for `DocIdx'. This
%%      will crash if all owning nodes are offline.
-spec first_up(DocIdx :: binary(),
               Service :: atom()) -> node().

%% TODO Not using the iterator it is now possible to return error instead of
%% crashing.
first_up(DocIdx, Service) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Preflist = riak_core_ring:preflist(DocIdx, Ring),
    UpSet =
        ordsets:from_list(riak_core_node_watcher:nodes(Service)),
    lists:nth(1,
              lists:filter(fun (Node) ->
                                   ordsets:is_element(Node, UpSet)
                           end,
                           Preflist)).

%% @doc Returns list of all owners that are curently not up.
-spec offline_owners(Service :: atom()) -> [{index(),
                                             node()}].

offline_owners(Service) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    offline_owners(Service, CHBin).

%% @doc Returns list of all owners that are curently not up.
-spec offline_owners(atom() | [node()],
                     CHBin :: chashbin()) -> [{index(), node()}].

offline_owners(Service, CHBin) when is_atom(Service) ->
    UpSet =
        ordsets:from_list(riak_core_node_watcher:nodes(Service)),
    offline_owners(UpSet, CHBin);
offline_owners(UpSet, CHBin) when is_list(UpSet) ->
    %% UpSet is an ordset of available nodes
    DownVNodes = chashbin:to_list_filter(fun ({_Index,
                                               Node}) ->
                                                 not is_up(Node, UpSet)
                                         end,
                                         CHBin),
    DownVNodes.

%% @doc Split a preference list into up and down lists.
-spec check_up(preflist(), [node()], preflist_ann(),
               preflist()) -> {preflist_ann(), preflist()}.

check_up([], _UpNodes, Up, Pangs) ->
    {lists:reverse(Up), lists:reverse(Pangs)};
check_up([{Partition, Node} | Rest], UpNodes, Up,
         Pangs) ->
    case is_up(Node, UpNodes) of
      true ->
          check_up(Rest, UpNodes,
                   [{{Partition, Node}, primary} | Up], Pangs);
      false ->
          check_up(Rest, UpNodes, Up, [{Partition, Node} | Pangs])
    end.

%% @doc Find fallbacks for downed nodes in the preference list.
-spec find_fallbacks(preflist(), preflist(), [node()],
                     preflist_ann()) -> preflist_ann().

find_fallbacks(_Pangs, [], _UpNodes, Secondaries) ->
    lists:reverse(Secondaries);
find_fallbacks([], _Fallbacks, _UpNodes, Secondaries) ->
    lists:reverse(Secondaries);
find_fallbacks([{Partition, _Node} | Rest] = Pangs,
               [{_, FN} | Fallbacks], UpNodes, Secondaries) ->
    case is_up(FN, UpNodes) of
      true ->
          find_fallbacks(Rest, Fallbacks, UpNodes,
                         [{{Partition, FN}, fallback} | Secondaries]);
      false ->
          find_fallbacks(Pangs, Fallbacks, UpNodes, Secondaries)
    end.

%% @doc Return true if a node is up.
is_up(Node, UpNodes) -> lists:member(Node, UpNodes).

%% @doc Return annotated preflist with partition ids/nums instead of hashes.
-spec apl_with_partition_nums(preflist_ann(),
                              riak_core_ring:ring_size()) -> preflist_with_pnum_ann().

%% WARN not sure if this still works with random slicing. Need to look into
%% riak_core_ring_util.
apl_with_partition_nums(Apl, Size) ->
    [{{riak_core_ring_util:hash_to_partition_id(Hash, Size),
       Node},
      Ann}
     || {{Hash, Node}, Ann} <- Apl].

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

smallest_test() ->
    Ring = riak_core_ring:fresh(node()),
    ?assertEqual([{0, node()}],
                 (get_apl(last_in_ring(), 1, Ring, [node()]))).

four_node_test() ->
    Nodes = [nodea, nodeb, nodec, noded],
    Ring = perfect_ring(8, Nodes),
    ?assertEqual([{0, nodea},
                  {182687704666362864775460604089535377456991567872,
                   nodeb},
                  {365375409332725729550921208179070754913983135744,
                   nodec}],
                 (get_apl(last_in_ring(), 3, Ring, Nodes))),
    %% With a node down
    ?assertEqual([{182687704666362864775460604089535377456991567872,
                   nodeb},
                  {365375409332725729550921208179070754913983135744,
                   nodec},
                  {0, noded}],
                 (get_apl(last_in_ring(), 3, Ring,
                          [nodeb, nodec, noded]))),
    %% With two nodes down
    ?assertEqual([{365375409332725729550921208179070754913983135744,
                   nodec},
                  {0, noded},
                  {182687704666362864775460604089535377456991567872,
                   nodec}],
                 (get_apl(last_in_ring(), 3, Ring, [nodec, noded]))),
    %% With the other two nodes down
    ?assertEqual([{0, nodea},
                  {182687704666362864775460604089535377456991567872,
                   nodeb},
                  {365375409332725729550921208179070754913983135744,
                   nodea}],
                 (get_apl(last_in_ring(), 3, Ring, [nodea, nodeb]))).

%% Create a perfect ring - RingSize must be a multiple of nodes
perfect_ring([H | _] = Nodes) ->
    Ring = riak_core_ring:fresh(H, Nodes),
    Ring.
    % Owners = riak_core_ring:all_owners(Ring),
    % TransferNode = fun ({Idx, _CurOwner},
    %                     {Ring0, [NewOwner | Rest]}) ->
    %                        {riak_core_ring:transfer_node(Idx, NewOwner, Ring0),
    %                         Rest ++ [NewOwner]}
    %                end,
    % {PerfectRing, _} = lists:foldl(TransferNode,
    %                                {Ring, Nodes}, Owners),
    % PerfectRing.

last_in_ring() -> hash:as_binary(hash:max_integer()).

six_node_test() ->
    %% its non-trivial to create a real 6 node ring, so here's one we made
    %% earlier
    {ok, [Ring]} = file:consult("test/my_ring"),
    %DocIdx = riak_core_util:chash_key({<<"foo">>, <<"bar">>}),
    DocIdx = <<73, 212, 27, 234, 104, 13, 150, 207, 0, 82,
               86, 183, 125, 225, 172, 154, 135, 46, 6, 112>>,
    Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1',
             'dev3@127.0.0.1', 'dev4@127.0.0.1', 'dev5@127.0.0.1',
             'dev6@127.0.0.1'],
    %% Fallbacks should be selected by finding the next-highest partition after
    %% the DocIdx of the key, in this case the 433883 partition. The N
    %% partitions at that point are the primary partitions. If any of the primaries
    %% are down, the next up node found by walking the preflist is used as the
    %% fallback for that partition.
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev2@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev3@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev4@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring, Nodes))),
    ?assertEqual([{456719261665907161938651510223838443642478919680,
                   'dev3@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev4@127.0.0.1'},
                  {433883298582611803841718934712646521460354973696,
                   'dev5@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes -- ['dev2@127.0.0.1']))),
    ?assertEqual([{479555224749202520035584085735030365824602865664,
                   'dev4@127.0.0.1'},
                  {433883298582611803841718934712646521460354973696,
                   'dev5@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev6@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes -- ['dev2@127.0.0.1', 'dev3@127.0.0.1']))),
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev5@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev6@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev1@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes --
                            ['dev2@127.0.0.1', 'dev3@127.0.0.1',
                             'dev4@127.0.0.1']))),
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev5@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev6@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev5@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes --
                            ['dev2@127.0.0.1', 'dev3@127.0.0.1',
                             'dev4@127.0.0.1', 'dev1@127.0.0.1']))),
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev2@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev3@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev5@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes -- ['dev4@127.0.0.1']))),
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev2@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev5@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev6@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes -- ['dev4@127.0.0.1', 'dev3@127.0.0.1']))),
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev2@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev5@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev1@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes --
                            ['dev4@127.0.0.1', 'dev3@127.0.0.1',
                             'dev6@127.0.0.1']))),
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev2@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev5@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev2@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes --
                            ['dev4@127.0.0.1', 'dev3@127.0.0.1',
                             'dev6@127.0.0.1', 'dev1@127.0.0.1']))),
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev2@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev2@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev2@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes --
                            ['dev4@127.0.0.1', 'dev3@127.0.0.1',
                             'dev6@127.0.0.1', 'dev1@127.0.0.1',
                             'dev5@127.0.0.1']))),
    ?assertEqual([{433883298582611803841718934712646521460354973696,
                   'dev2@127.0.0.1'},
                  {479555224749202520035584085735030365824602865664,
                   'dev4@127.0.0.1'},
                  {456719261665907161938651510223838443642478919680,
                   'dev5@127.0.0.1'}],
                 (get_apl(DocIdx, 3, Ring,
                          Nodes -- ['dev3@127.0.0.1']))),
    ok.

six_node_bucket_key_ann_test() ->
    {ok, [Ring]} = file:consult("test/my_ring"),
    Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1',
             'dev3@127.0.0.1', 'dev4@127.0.0.1', 'dev5@127.0.0.1',
             'dev6@127.0.0.1'],
    Bucket = <<"favorite">>,
    Key = <<"jethrotull">>,
    application:set_env(riak_core, target_n_val, 3),
    riak_core_ring_manager:setup_ets(test),
    riak_core_ring_manager:set_ring_global(Ring),
    Size = riak_core_ring:num_partitions(Ring),
    ?assertEqual([{{34, 'dev5@127.0.0.1'}, primary},
                  {{35, 'dev6@127.0.0.1'}, primary},
                  {{36, 'dev1@127.0.0.1'}, primary}],
                 (apl_with_partition_nums(get_apl_ann({Bucket, Key},
                                                      Nodes),
                                          Size))),
    ?assertEqual([{{35, 'dev6@127.0.0.1'}, primary},
                  {{36, 'dev1@127.0.0.1'}, primary},
                  {{34, 'dev2@127.0.0.1'}, fallback}],
                 (apl_with_partition_nums(get_apl_ann({Bucket, Key},
                                                      Nodes --
                                                        ['dev5@127.0.0.1']),
                                          Size))),
    ?assertEqual([{{36, 'dev1@127.0.0.1'}, primary},
                  {{34, 'dev2@127.0.0.1'}, fallback},
                  {{35, 'dev3@127.0.0.1'}, fallback}],
                 (apl_with_partition_nums(get_apl_ann({Bucket, Key},
                                                      Nodes --
                                                        ['dev5@127.0.0.1',
                                                         'dev6@127.0.0.1']),
                                          Size))),
    ?assertEqual([{{34, 'dev2@127.0.0.1'}, fallback},
                  {{35, 'dev3@127.0.0.1'}, fallback},
                  {{36, 'dev4@127.0.0.1'}, fallback}],
                 (apl_with_partition_nums(get_apl_ann({Bucket, Key},
                                                      Nodes --
                                                        ['dev5@127.0.0.1',
                                                         'dev6@127.0.0.1',
                                                         'dev1@127.0.0.1']),
                                          Size))),
    ?assertEqual([{{34, 'dev3@127.0.0.1'}, fallback},
                  {{35, 'dev4@127.0.0.1'}, fallback},
                  {{36, 'dev3@127.0.0.1'}, fallback}],
                 (apl_with_partition_nums(get_apl_ann({Bucket, Key},
                                                      Nodes --
                                                        ['dev5@127.0.0.1',
                                                         'dev6@127.0.0.1',
                                                         'dev1@127.0.0.1',
                                                         'dev2@127.0.0.1']),
                                          Size))),
    ?assertEqual([{{34, 'dev4@127.0.0.1'}, fallback},
                  {{35, 'dev4@127.0.0.1'}, fallback},
                  {{36, 'dev4@127.0.0.1'}, fallback}],
                 (apl_with_partition_nums(get_apl_ann({Bucket, Key},
                                                      Nodes --
                                                        ['dev5@127.0.0.1',
                                                         'dev6@127.0.0.1',
                                                         'dev1@127.0.0.1',
                                                         'dev2@127.0.0.1',
                                                         'dev3@127.0.0.1']),
                                          Size))),
    ?assertEqual([{{34, 'dev5@127.0.0.1'}, primary},
                  {{35, 'dev6@127.0.0.1'}, primary},
                  {{36, 'dev3@127.0.0.1'}, fallback}],
                 (apl_with_partition_nums(get_apl_ann({Bucket, Key},
                                                      Nodes --
                                                        ['dev1@127.0.0.1',
                                                         'dev2@127.0.0.1']),
                                          Size))),
    riak_core_ring_manager:cleanup_ets(test),
    ok.

chbin_test_() ->
    {timeout, 180, fun chbin_test_scenario/0}.

chbin_test_scenario() ->
    [chbin_test_scenario(Size, NumNodes)
     || Size <- [32, 64, 128],
        NumNodes <- [1, 2, 3, 4, 5, 8, Size div 4]],
    ok.

chbin_test_scenario(Size, NumNodes) ->
    RingTop = 1 bsl 160,
    Ring = riak_core_test_util:fake_ring(Size, NumNodes),
    Nodes = riak_core_ring:all_members(Ring),
    CHash = riak_core_ring:chash(Ring),
    CHBin = chashbin:create(CHash),
    Inc = chash:ring_increment(Size),
    HashKeys = [hash:as_binary(X)
                || X <- lists:seq(0, RingTop, Inc div 2)],
    Shuffled = riak_core_util:shuffle(Nodes),
    _ = CHBin,
    [begin
       Up = max(0, NumNodes - Down),
       UpNodes = lists:sublist(Shuffled, Up),
       ?assertEqual((get_apl(HashKey, N, Ring, UpNodes)),
                    (get_apl_chbin(HashKey, N, CHBin, UpNodes))),
       ?assertEqual((get_primary_apl(HashKey, N, Ring,
                                     UpNodes)),
                    (get_primary_apl_chbin(HashKey, N, CHBin, UpNodes))),
       ok
     end
     || HashKey <- HashKeys, N <- [1, 2, 3, 4],
        Down <- [0, 1, 2, Size div 2, Size - 1, Size]],
    ok.

-endif.

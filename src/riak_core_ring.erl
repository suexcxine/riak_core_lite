%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc riak_core_ring manages a riak node's local view of partition ownership.
%%      The functions in this module revolve around use of the chstate record,
%%      which should be treated as opaque by other modules.  Riak nodes exchange
%%      instances of these records via gossip in order to converge on a common
%%      view of node/partition ownership.

-module(riak_core_ring).

-export([all_members/1, all_owners/1, all_preflists/2,
         diff_nodes/2, equal_rings/2, fresh/0, fresh/1, fresh/2,
         get_meta/2, index_owner/2, my_indices/1,
         num_partitions/1, owner_node/1, preflist/2,
         random_node/1, random_other_index/1,
         random_other_index/2, random_other_node/1, reconcile/2,
         rename_node/3, responsible_index/2, transfer_node/3,
         compact_ring/1, update_meta/3, remove_meta/2]).

-export([cluster_name/1, set_tainted/1, check_tainted/2,
         nearly_equal/2, claimant/1, member_status/2,
         pretty_print/2, all_member_status/1,
         update_member_meta/5, clear_member_meta/3,
         get_member_meta/3, add_member/3, remove_member/3,
         leave_member/3, exit_member/3, down_member/3,
         set_member/4, set_member/5, members/2, set_claimant/2,
         increment_vclock/2, ring_version/1,
         increment_ring_version/2, set_pending_changes/2,
         active_members/1, claiming_members/1, ready_members/1,
         random_other_active_node/1, down_members/1, set_owner/2,
         indices/2, future_indices/2, future_ring/1,
         disowning_indices/2, cancel_transfers/1,
         pending_changes/1, next_owner/1, next_owner/2,
         next_owner/3, completed_next_owners/2,
         all_next_owners/1, change_owners/2, handoff_complete/3,
         ring_ready/0, ring_ready/1, ring_ready_info/1,
         ring_changed/2, set_cluster_name/2, reconcile_names/2,
         reconcile_members/2, is_primary/2, chash/1, set_chash/2,
         future_index/3, future_index/4, is_future_index/4,
         future_owner/2, future_num_partitions/1, vnode_type/2,
         deletion_complete/3, get_weight/2, get_weights/1]).

                               %%         upgrade/1,
                               %%         downgrade/2,

-export_type([riak_core_ring/0, ring_size/0,
              partition_id/0]).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

-record(chstate,
        {nodename  ::
             term(),          % the Node responsible for this chstate
         vclock  ::
             vclock:vclock() |
             undefined, % for this chstate object, entries are
         % {Node, Ctr}
         chring  ::
             chash:chash() |
             undefined,   % chash ring of {IndexAsInt, Node} mappings
         meta  :: dict:dict() | undefined,
         % dict of cluster-wide other data (primarily N-value, etc)
         clustername  :: {term(), term()} | undefined,
         next  ::
             [{integer(), term(), term(), [module()],
               awaiting | complete}],
         members  ::
             [{node(),
               {member_status(), vclock:vclock(),
                [{atom(), term()}]}}] |
             undefined,
         claimant  :: term(),
         seen  :: [{term(), vclock:vclock()}] | undefined,
         rvsn  :: vclock:vclock() | undefined}).

-type member_status() :: joining | valid | invalid |
                         leaving | exiting | down.

%% type meta_entry(). Record for each entry in #chstate.meta
-record(meta_entry,
        {value,    % The value stored under this entry
         lastmod}).   % The last modified time of this entry,
                      %  from calendar:datetime_to_gregorian_seconds(
                      %                             calendar:universal_time()),

%% @type riak_core_ring(). Opaque data type used for partition ownership
-type riak_core_ring() :: #chstate{}.

-type chstate() :: riak_core_ring().

-type pending_change() :: {Owner :: node(),
                           NextOwner :: node(), awaiting | complete} |
                          {undefined, undefined, undefined}.

-type ring_size() :: non_neg_integer().

%% @type partition_id(). This integer represents a value in the range [0, ring_size-1].
-type partition_id() :: non_neg_integer().

-type preflist() :: [{integer(), term()}].

-define(DEFAULT_WEIGHT, 100).

%% ===================================================================
%% Public API
%% ===================================================================

set_tainted(Ring) ->
    update_meta(riak_core_ring_tainted, true, Ring).

check_tainted(Ring = #chstate{}, Msg) ->
    Exit = application:get_env(riak_core, exit_when_tainted,
                               false),
    case {get_meta(riak_core_ring_tainted, Ring), Exit} of
      {{ok, true}, true} -> riak_core:stop(Msg), ok;
      {{ok, true}, false} -> logger:error(Msg), ok;
      _ -> ok
    end.

%% @doc Verify that the two rings are identical expect that metadata can
%%      differ and RingB's vclock is allowed to be equal or a direct
%%      descendant of RingA's vclock. This matches the changes that the
%%      fix-up logic may make to a ring.
-spec nearly_equal(chstate(), chstate()) -> boolean().

nearly_equal(RingA, RingB) ->
    TestVC = vclock:descends(RingB#chstate.vclock,
                             RingA#chstate.vclock),
    RingA2 = RingA#chstate{vclock = undefined,
                           meta = undefined},
    RingB2 = RingB#chstate{vclock = undefined,
                           meta = undefined},
    TestRing = RingA2 =:= RingB2,
    TestVC and TestRing.

%% @doc Determine if a given Index/Node `IdxNode' combination is a
%%      primary.
-spec is_primary(chstate(),
                 {chash:index_as_int(), node()}) -> boolean().

is_primary(Ring, IdxNode) ->
    Owners = all_owners(Ring),
    lists:member(IdxNode, Owners).

%% @doc Return the `CHash' of the ring.
-spec chash(chstate()) -> CHash :: chash:chash().

chash(#chstate{chring = CHash}) -> CHash.

set_chash(State, CHash) ->
    State#chstate{chring = CHash}.

%% @doc Produce a list of all nodes that are members of the cluster
-spec all_members(State :: chstate()) -> [Node ::
                                              term()].

all_members(#chstate{members = Members}) ->
    get_members(Members).

%% @doc Produce a list of all nodes in the cluster with the given types
-spec members(State :: chstate(),
              Types :: [member_status()]) -> [Node :: term()].

members(#chstate{members = Members}, Types) ->
    get_members(Members, Types).

%% @doc Produce a list of all active (not marked as down) cluster members
-spec active_members(State :: chstate()) -> [Node ::
                                                 term()].

active_members(#chstate{members = Members}) ->
    get_members(Members,
                [joining, valid, leaving, exiting]).

%% @doc Returns a list of members guaranteed safe for requests
-spec ready_members(State :: chstate()) -> [Node ::
                                                term()].

ready_members(#chstate{members = Members}) ->
    get_members(Members, [valid, leaving]).

%% @doc Provide all ownership information in the form of {Index,Node} pairs.
-spec all_owners(State :: chstate()) -> [{Index ::
                                              integer(),
                                          Node :: term()}].

all_owners(State) ->
    [{hash:as_integer(I), N}
     || {I, N} <- chash:nodes(State#chstate.chring)].

%% @doc Provide every preflist in the ring, truncated at N.
%% @deprecated There is now a unique preference list per element of the hash
%%             space instead of per partition. Computing all preflists is not
%%             feasible.
-spec all_preflists(State :: chstate(),
                    N :: integer()) -> [preflist()].

all_preflists(State, N) ->
    [lists:sublist(preflist(Key, State), N)
     || Key
            <- [hash:as_binary(I + 1)
                || {I, _Owner} <- (?MODULE):all_owners(State)]].

%% @doc For two rings, return the list of owners that have differing ownership.
-spec diff_nodes(chstate(), chstate()) -> [node()].

                                        %% node() not consistent with chash implementation

diff_nodes(State1, State2) ->
    AO = lists:zip(all_owners(State1), all_owners(State2)),
    AllDiff = [[N1, N2]
               || {{I, N1}, {I, N2}} <- AO, N1 =/= N2],
    lists:usort(lists:flatten(AllDiff)).

-spec equal_rings(chstate(), chstate()) -> boolean().

equal_rings(_A = #chstate{chring = RA, meta = MA},
            _B = #chstate{chring = RB, meta = MB}) ->
    MDA = lists:sort(dict:to_list(MA)),
    MDB = lists:sort(dict:to_list(MB)),
    case MDA =:= MDB of
      false -> false;
      true -> RA =:= RB
    end.

%% @doc This is used only when this node is creating a brand new cluster.
-spec fresh() -> chstate().

fresh() ->
    % use this when starting a new cluster via this node
    fresh(node()).

%% @doc Equivalent to fresh/0 but allows specification of the local node name.
-spec fresh(NodeName :: term()) -> chstate().

fresh(NodeName) -> fresh(NodeName, [NodeName]).

%% @doc Equivalent to fresh/1 but allows specification of other nodes.
%%      Called by fresh/1, and otherwise only intended for testing purposes.
%%      The name of the local node needs to be the first entry in the list of
%%      nodes.
-spec fresh(LocalName :: term(),
            Nodes :: [term()]) -> chstate().

fresh(LocalName, Nodes = [LocalName | _]) ->
    VClock = vclock:increment(LocalName, vclock:fresh()),
    #chstate{nodename = LocalName,
             clustername = {LocalName, erlang:timestamp()},
             members =
                 [{Name,
                   {valid, VClock, [{gossip_vsn, 2}, {weight, 100}]}}
                  || Name <- Nodes],
             chring = chash:fresh([{Node, 100} || Node <- Nodes]),
             next = [], claimant = LocalName,
             seen = [{LocalName, VClock}], rvsn = VClock,
             vclock = VClock, meta = dict:new()}.

% @doc Return a value from the cluster metadata dict
-spec get_meta(Key :: term(),
               State :: chstate()) -> {ok, term()} | undefined.

get_meta(Key, State) ->
    case dict:find(Key, State#chstate.meta) of
      error -> undefined;
      {ok, '$removed'} -> undefined;
      {ok, M} when M#meta_entry.value =:= '$removed' ->
          undefined;
      {ok, M} -> {ok, M#meta_entry.value}
    end.

%% @doc Return the node that owns the given index.
-spec index_owner(State :: chstate(),
                  Idx :: chash:index_as_int()) -> Node :: term() |
                                                          not_exisitng.

index_owner(State, Idx) ->
    case lists:keyfind(Idx, 1, all_owners(State)) of
      {Idx, Owner} -> Owner;
      false -> not_exisitng
    end.

%% @doc Return the node that will own this index after transtions have completed
%%      this function will error if the ring is shrinking and Idx no longer
%%      exists in it
-spec future_owner(chstate(),
                   chash:index_as_int()) -> term() | not_exisitng.

future_owner(State, Idx) ->
    index_owner(future_ring(State), Idx).

%% @doc Return all partition indices owned by the node executing this function.
-spec my_indices(State ::
                     chstate()) -> [chash:index_as_int()].

my_indices(State) ->
    [I
     || {I, Owner} <- (?MODULE):all_owners(State),
        Owner =:= node()].

%% @doc Return the number of partitions in this Riak ring.
-spec num_partitions(State ::
                         chstate()) -> pos_integer().

num_partitions(State) ->
    chash:size(State#chstate.chring).

-spec future_num_partitions(chstate()) -> pos_integer().

future_num_partitions(State) ->
    num_partitions(future_ring(State)).

%% @doc Return the node that is responsible for a given chstate.
-spec owner_node(State :: chstate()) -> Node :: term().

owner_node(State) -> State#chstate.nodename.

%% @doc For a given object key, produce the ordered list of
%%      {partition,node} pairs that could be responsible for that object.
-spec preflist(Key :: binary(),
               State :: chstate()) -> [{Index :: chash:index_as_int(),
                                        Node :: term()}].

preflist(Key, State) ->
    PrefList = replication:replicate(hash:as_integer(Key),
                                     length(chash:members(State#chstate.chring)),
                                     State),
    [{hash:as_integer(I), N} || {I, N} <- PrefList].

%% @doc Return a randomly-chosen node from amongst the owners.
-spec random_node(State :: chstate()) -> Node :: term().

random_node(State) ->
    L = all_members(State),
    lists:nth(rand:uniform(length(L)), L).

%% @doc Return a partition index not owned by the node executing this function.
%%      If this node owns all partitions, return any index.
-spec random_other_index(State ::
                             chstate()) -> chash:index_as_int().

random_other_index(State) ->
    L = [I
         || {I, Owner} <- (?MODULE):all_owners(State),
            Owner =/= node()],
    case L of
      [] -> hd(my_indices(State));
      _ -> lists:nth(rand:uniform(length(L)), L)
    end.

%% @doc Return a partition index not owned by the node executing this function
%%      or contained in the exclude list.
%%      If there are no feasible index return no_indices.
-spec random_other_index(State :: chstate(),
                         Exclude :: [term()]) -> chash:index_as_int() |
                                                 no_indices.

random_other_index(State, Exclude)
    when is_list(Exclude) ->
    L = [I
         || {I, Owner} <- (?MODULE):all_owners(State),
            Owner =/= node(), not lists:member(I, Exclude)],
    case L of
      [] -> no_indices;
      _ -> lists:nth(rand:uniform(length(L)), L)
    end.

%% @doc Return a randomly-chosen node from amongst the owners other than this one.
-spec random_other_node(State :: chstate()) -> Node ::
                                                   term() | no_node.

random_other_node(State) ->
    case lists:delete(node(), all_members(State)) of
      [] -> no_node;
      L -> lists:nth(rand:uniform(length(L)), L)
    end.

%% @doc Return a randomly-chosen active node other than this one.
-spec random_other_active_node(State ::
                                   chstate()) -> Node :: term() | no_node.

random_other_active_node(State) ->
    case lists:delete(node(), active_members(State)) of
      [] -> no_node;
      L -> lists:nth(rand:uniform(length(L)), L)
    end.

%% @doc Incorporate another node's state into our view of the Riak world.
-spec reconcile(ExternState :: chstate(),
                MyState :: chstate()) -> {no_change | new_ring,
                                          chstate()}.

reconcile(ExternState, MyState) ->
    check_tainted(ExternState,
                  "Error: riak_core_ring/reconcile :: reconcilin"
                  "g tainted external ring"),
    check_tainted(MyState,
                  "Error: riak_core_ring/reconcile :: reconcilin"
                  "g tainted internal ring"),
    case internal_reconcile(MyState, ExternState) of
      {false, State} -> {no_change, State};
      {true, State} -> {new_ring, State}
    end.

%% @doc  Rename OldNode to NewNode in a Riak ring.
-spec rename_node(State :: chstate(), OldNode :: atom(),
                  NewNode :: atom()) -> chstate().

rename_node(State = #chstate{chring = Ring,
                             nodename = ThisNode, members = Members,
                             claimant = Claimant, seen = Seen},
            OldNode, NewNode)
    when is_atom(OldNode), is_atom(NewNode) ->
    State#chstate{chring =
                      lists:foldl(fun ({Idx, Owner}, AccIn) ->
                                          case Owner of
                                            OldNode ->
                                                chash:update(Idx, NewNode,
                                                             AccIn);
                                            _ -> AccIn
                                          end
                                  end,
                                  Ring, riak_core_ring:all_owners(State)),
                  members =
                      orddict:from_list(proplists:substitute_aliases([{OldNode,
                                                                       NewNode}],
                                                                     Members)),
                  seen =
                      orddict:from_list(proplists:substitute_aliases([{OldNode,
                                                                       NewNode}],
                                                                     Seen)),
                  nodename =
                      case ThisNode of
                        OldNode -> NewNode;
                        _ -> ThisNode
                      end,
                  claimant =
                      case Claimant of
                        OldNode -> NewNode;
                        _ -> Claimant
                      end,
                  vclock =
                      vclock:increment(NewNode, State#chstate.vclock)}.

%% @doc Determine the integer ring index responsible
%%      for a chash key.
-spec responsible_index(binary(),
                        chstate()) -> integer().

responsible_index(ChashKey, #chstate{chring = Ring}) ->
    IndexAsInt = hash:as_integer(ChashKey),
    chash:next_index(IndexAsInt, Ring).

%% @doc Given a key and an index in the current ring, determine
%%      which index will own the key in the future ring. `OrigIdx'
%%      may or may not be the responsible index for that key
%%      (`OrigIdx' may not be the first index in `CHashKey''s preflist).
%%      The returned index will be in the same position in the preflist
%%      for `CHashKey' in the future ring. For regular transitions
%%      the returned index will always be `OrigIdx'.
-spec future_index(CHashKey :: chash:index(),
                   OridIdx :: integer(), State :: chstate()) -> integer() |
                                                                undefined.

future_index(CHashKey, OrigIdx, State) ->
    future_index(CHashKey, OrigIdx, undefined, State).

%% @doc Determine which ring index will own the given key in the future ring.
%% @param ChashKey The key for which the future owner is to be determined.
%% @param OrigIdx Index of the original key owner.
%% @param NValCheck Upper bound for N if defined.
%% @returns The future index if it is valid, else `undefined'.
%% @see future_index/3
-spec future_index(CHashKey :: chash:index(),
                   OrigIdx :: integer(),
                   NValCheck :: undefined | integer(),
                   State :: chstate()) -> integer() | undefined.

future_index(CHashKey, OrigIdx, NValCheck, State) ->
    OrigPrefList = preflist(CHashKey, State),
    NextPrefList = preflist(CHashKey, future_ring(State)),
    %% Determine position of the source partition in the preference list
    {OrigPos, Found} = lists:foldl(fun ({Idx, _},
                                        {Pos, Found}) ->
                                           case {Found, Idx =:= OrigIdx} of
                                             {true, _} -> {Pos, true};
                                             {false, false} -> {Pos + 1, false};
                                             {false, true} -> {Pos, true}
                                           end
                                   end,
                                   {0, false}, OrigPrefList),
    NextCount = length(NextPrefList),
    Invalid = check_invalid_future_index(OrigPos, NextCount,
                                         NValCheck),
    case {Found, Invalid} of
      {false, _} -> undefined;
      {_, true} -> undefined;
      {true, false} ->
          %% Determine the partition that the key should be transferred to (has same position
          %% in future preflist as source partition does in current preflist)
          {NextIdx, _} = lists:nth(OrigPos + 1, NextPrefList),
          NextIdx
    end.

%% @doc Check if the index is either out of bounds of the ring size or the n
%% value
-spec check_invalid_future_index(non_neg_integer(),
                                 pos_integer(),
                                 integer() | undefined) -> boolean().

check_invalid_future_index(OrigPos, NextCount,
                           NValCheck) ->
    OverRingSize = OrigPos >= NextCount,
    OverNVal = case NValCheck of
                 undefined -> false;
                 _ -> OrigPos >= NValCheck
               end,
    OverRingSize orelse OverNVal.

%% @doc Takes the hashed value for a key and any partition, `OrigIdx',
%% in the current preflist for the key. Returns true if `TargetIdx'
%% is in the same position in the future preflist for that key.
%% @see future_index/4
-spec is_future_index(chash:index(), integer(),
                      integer(), chstate()) -> boolean().

is_future_index(CHashKey, OrigIdx, TargetIdx, State) ->
    FutureIndex = future_index(CHashKey, OrigIdx, undefined,
                               State),
    FutureIndex =:= TargetIdx.

%% @doc Transfers the ownership of an index to another node.
%% @param Idx Index to be owned by the node.
%% @param Node Name of the node to own the index.
%% @param State Current state of the ring.
%% @returns Current state if the node already owns the index, the updated state
%% with an increased vector clock otherwise.
-spec transfer_node(Idx :: integer(), Node :: term(),
                    MyState :: chstate()) -> chstate().

transfer_node(Idx, Node, MyState) ->
    N = index_owner(MyState, Idx),
    case N of
      Node -> MyState;
      not_existing ->
          logger:warning("Transfer node attempted for non existant "
                         "index ~p",
                         [Idx]),
          MyState;
      _ ->
          Me = MyState#chstate.nodename,
          VClock = vclock:increment(Me, MyState#chstate.vclock),
          CHRing = chash:update(Idx, Node,
                                MyState#chstate.chring),
          MyState#chstate{vclock = VClock, chring = CHRing}
    end.

%% @doc Merges neighboring sections owned by the same node.
%% @param Ring Ring to compact.
%% @returns Compacted ring.
-spec compact_ring(Ring :: chstate()) -> chstate().

compact_ring(Ring = #chstate{chring = CHash}) ->
    Ring#chstate{chring = chash:compact(CHash)}.

%% @doc Set a key in the cluster metadata dict
-spec update_meta(Key :: term(), Val :: term(),
                  State :: chstate()) -> chstate().

update_meta(Key, Val, State) ->
    Change = case dict:find(Key, State#chstate.meta) of
               {ok, OldM} -> Val /= OldM#meta_entry.value;
               error -> true
             end,
    if Change ->
           M = #meta_entry{lastmod =
                               calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
                           value = Val},
           VClock = vclock:increment(State#chstate.nodename,
                                     State#chstate.vclock),
           State#chstate{vclock = VClock,
                         meta = dict:store(Key, M, State#chstate.meta)};
       true -> State
    end.

%% @doc Logical delete of a key in the cluster metadata dict
-spec remove_meta(Key :: term(),
                  State :: chstate()) -> chstate().

remove_meta(Key, State) ->
    case dict:find(Key, State#chstate.meta) of
      {ok, _} -> update_meta(Key, '$removed', State);
      error -> State
    end.

%% @doc Return the current claimant.
-spec claimant(State :: chstate()) -> node().

claimant(#chstate{claimant = Claimant}) -> Claimant.

%% @doc Set the new claimant.
-spec set_claimant(State :: chstate(),
                   Claimant :: node()) -> NState :: chstate().

set_claimant(State, Claimant) ->
    State#chstate{claimant = Claimant}.

%% @doc Returns the unique identifer for this cluster.
-spec cluster_name(State :: chstate()) -> term().

cluster_name(State) -> State#chstate.clustername.

%% @doc Sets the unique identifer for this cluster.
-spec set_cluster_name(State :: chstate(),
                       Name :: {term(), term()}) -> chstate().

set_cluster_name(State, Name) ->
    State#chstate{clustername = Name}.

%% @doc Mark the cluster names as undefined if at least one is undefined.
%% Else leave the names unchanged.
-spec reconcile_names(RingA :: chstate(),
                      RingB :: chstate()) -> {chstate(), chstate()}.

reconcile_names(RingA = #chstate{clustername = NameA},
                RingB = #chstate{clustername = NameB}) ->
    case (NameA =:= undefined) or (NameB =:= undefined) of
      true ->
          {RingA#chstate{clustername = undefined},
           RingB#chstate{clustername = undefined}};
      false -> {RingA, RingB}
    end.

%% @doc Increment the vector clock and return the new state.
-spec increment_vclock(Node :: node(),
                       State :: chstate()) -> chstate().

increment_vclock(Node, State) ->
    VClock = vclock:increment(Node, State#chstate.vclock),
    State#chstate{vclock = VClock}.

%% @doc Return the current ring version.
-spec ring_version(chstate()) -> vclock:vclock() |
                                 undefined.

ring_version(#chstate{rvsn = RVsn}) -> RVsn.

%% @doc Increment the ring version and return the new state.
-spec increment_ring_version(node(),
                             chstate()) -> chstate().

increment_ring_version(Node, State) ->
    RVsn = vclock:increment(Node, State#chstate.rvsn),
    State#chstate{rvsn = RVsn}.

%% @doc Returns the current membership status for a node in the cluster.
-spec member_status(chstate() | [node()],
                    Node :: node()) -> member_status().

member_status(#chstate{members = Members}, Node) ->
    member_status(Members, Node);
member_status(Members, Node) ->
    case orddict:find(Node, Members) of
      {ok, {Status, _, _}} -> Status;
      _ -> invalid
    end.

%% @doc Returns the current membership status for all nodes in the cluster.
-spec all_member_status(State :: chstate()) -> [{node(),
                                                 member_status()}].

all_member_status(#chstate{members = Members}) ->
    [{Node, Status}
     || {Node, {Status, _VC, _}} <- Members,
        Status /= invalid].

%% @doc return the member's meta value for the given key or undefined if the
%% member or key cannot be found.
-spec get_member_meta(chstate(), node(),
                      atom()) -> term() | undefined.

get_member_meta(State, Member, Key) ->
    case orddict:find(Member, State#chstate.members) of
      error -> undefined;
      {ok, {_, _, Meta}} ->
          case orddict:find(Key, Meta) of
            error -> undefined;
            {ok, Value} -> Value
          end
    end.

%% @doc Set a key in the member metadata orddict
-spec update_member_meta(node(), chstate(), node(),
                         atom(), term()) -> chstate().

update_member_meta(Node, State, Member, Key, Val) ->
    VClock = vclock:increment(Node, State#chstate.vclock),
    State2 = update_member_meta(Node, State, Member, Key,
                                Val, same_vclock),
    State2#chstate{vclock = VClock}.

%% @doc Updates the vector clock and meta data entry for the given member.
%% @see update_member_meta/5.
-spec update_member_meta(node(), chstate(), node(),
                         atom(), term(), same_vclock) -> chstate().

update_member_meta(Node, State, Member, Key, Val,
                   same_vclock) ->
    Members = State#chstate.members,
    case orddict:is_key(Member, Members) of
      true ->
          Members2 = orddict:update(Member,
                                    fun ({Status, VC, MD}) ->
                                            {Status, vclock:increment(Node, VC),
                                             orddict:store(Key, Val, MD)}
                                    end,
                                    Members),
          State#chstate{members = Members2};
      false -> State
    end.

%% @doc Remove the meta entries for the given member.
-spec clear_member_meta(node(), chstate(),
                        node()) -> chstate().

clear_member_meta(Node, State, Member) ->
    Members = State#chstate.members,
    case orddict:is_key(Member, Members) of
      true ->
          Members2 = orddict:update(Member,
                                    fun ({Status, VC, _MD}) ->
                                            {Status, vclock:increment(Node, VC),
                                             orddict:new()}
                                    end,
                                    Members),
          State#chstate{members = Members2};
      false -> State
    end.

%% @doc Mark a member as joining
-spec add_member(node(), chstate(),
                 node()) -> chstate().

add_member(PNode, State, Node) ->
    State2 = set_member(PNode, State, Node, joining),
    %% Set weight, which is currently not considered outside of ring and chash.
    update_member_meta(PNode, State2, Node, weight,
                       ?DEFAULT_WEIGHT).

%% @doc Mark a member as invalid
-spec remove_member(node(), chstate(),
                    node()) -> chstate().

remove_member(PNode, State, Node) ->
    State2 = clear_member_meta(PNode, State, Node),
    set_member(PNode, State2, Node, invalid).

%% @doc Mark a member as leaving
-spec leave_member(node(), chstate(),
                   node()) -> chstate().

leave_member(PNode, State, Node) ->
    set_member(PNode, State, Node, leaving).

%% @doc Mark a member as exiting
-spec exit_member(node(), chstate(),
                  node()) -> chstate().

exit_member(PNode, State, Node) ->
    set_member(PNode, State, Node, exiting).

%% @doc Mark a member as down
-spec down_member(node(), chstate(),
                  node()) -> chstate().

down_member(PNode, State, Node) ->
    set_member(PNode, State, Node, down).

%% @doc Mark a member with the given status
-spec set_member(node(), chstate(), node(),
                 member_status()) -> chstate().

set_member(Node, CState, Member, Status) ->
    VClock = vclock:increment(Node, CState#chstate.vclock),
    CState2 = set_member(Node, CState, Member, Status,
                         same_vclock),
    CState2#chstate{vclock = VClock}.

%% @doc Update the vector clock and status for the member.
-spec set_member(node(), chstate(), node(),
                 member_status(), same_vclock) -> chstate().

set_member(Node, CState, Member, Status, same_vclock) ->
    Members2 = orddict:update(Member,
                              fun ({_, VC, MD}) ->
                                      {Status, vclock:increment(Node, VC), MD}
                              end,
                              {Status, vclock:increment(Node, vclock:fresh()),
                               []},
                              CState#chstate.members),
    CState#chstate{members = Members2}.

%% @doc Return a list of all members of the cluster that are eligible to
%%      claim partitions.
-spec claiming_members(State :: chstate()) -> [Node ::
                                                   node()].

claiming_members(#chstate{members = Members}) ->
    get_members(Members, [joining, valid, down]).

%% @doc Return a list of all members of the cluster that are marked as down.
-spec down_members(State :: chstate()) -> [Node ::
                                               node()].

down_members(#chstate{members = Members}) ->
    get_members(Members, [down]).

%% @doc Set the node that is responsible for a given chstate.
-spec set_owner(State :: chstate(),
                Node :: node()) -> chstate().

set_owner(State, Node) ->
    State#chstate{nodename = Node}.

%% @doc Return all partition indices owned by a node.
-spec indices(State :: chstate(),
              Node :: node()) -> [integer()].

indices(State, Node) ->
    AllOwners = all_owners(State),
    [Idx || {Idx, Owner} <- AllOwners, Owner =:= Node].

%% @doc Return all partition indices that will be owned by a node after all
%%      pending ownership transfers have completed.
-spec future_indices(State :: chstate(),
                     Node :: node()) -> [integer()].

future_indices(State, Node) ->
    indices(future_ring(State), Node).

%% @doc Return all node entries that will exist after the pending changes are
%% applied.
-spec all_next_owners(chstate()) -> [{integer(),
                                      term()}].

all_next_owners(CState) ->
    Next = riak_core_ring:pending_changes(CState),
    [{Idx, NextOwner} || {Idx, _, NextOwner, _, _} <- Next].

%% @private
%% Change the owner of the indices to the new owners.
-spec change_owners(chstate(),
                    [{integer(), node()}]) -> chstate().

change_owners(CState, Reassign) ->
    lists:foldl(fun ({Idx, NewOwner}, CState0) ->
                        riak_core_ring:transfer_node(Idx, NewOwner, CState0)
                end,
                CState, Reassign).

%% @doc Return all indices that a node is scheduled to give to another.
-spec disowning_indices(chstate(),
                        node()) -> [integer()].

disowning_indices(State, Node) ->
    [Idx
     || {Idx, Owner, _NextOwner, _Mods, _Status}
            <- State#chstate.next,
        Owner =:= Node].

%% @doc Returns a list of all pending ownership transfers.
-spec pending_changes(chstate()) -> [{integer(), term(),
                                      term(), [module()], awaiting | complete}].

pending_changes(State) ->
    %% For now, just return next directly.
    State#chstate.next.

%% @doc Set the transfers as pending changes
-spec set_pending_changes(chstate(),
                          [{integer(), term(), term(), [module()],
                            awaiting | complete}]) -> chstate().

set_pending_changes(State, Transfers) ->
    State#chstate{next = Transfers}.

%% @doc Update the next owner list of the given index with the given module
%%      being transferred.
%% @param State State to work on.
%% @param Idx Index to be updated.
%% @param Mod VNode module to be marked as transferred.
%% @returns Te updated state where the module is marked as transferred and the
%%          status of the next owners for the given index is updated.
-spec deletion_complete(State :: chstate(),
                        Idx :: integer(), Mod :: atom()) -> chstate().

deletion_complete(State, Idx, Mod) ->
    transfer_complete(State, Idx, Mod).

%% @doc Retrieve the type of the vnode at the given index.
%% @param State State to work on.
%% @param Idx Index of the vnode.
%% @returns Type of the vnode, in case of `fallback' also the name of the
%%          primary node.
-spec vnode_type(chstate(), integer()) -> primary |
                                          {fallback, term()} | future_primary.

vnode_type(State, Idx) ->
    vnode_type(State, Idx, node()).

%% @doc Determine the vnode type of a given index on a given node.
%% @param State State to work on.
%% @param Idx Index of the vnode.
%% @param Node Primary owner node to be used as a reference.
%% @returns Type of the virtual node. If the owner of the index is the given
%%          `Node' `primary', otherwise `future_primary' or fallback. If the index does not exist on the ring `undefined'
-spec vnode_type(State :: chstate(), Idx :: integer(),
                 Node :: term()) -> primary | {fallback, term()} |
                                    future_primary | undefined.

vnode_type(State, Idx, Node) ->
    case index_owner(State, Idx) of
      Node -> primary;
      not_existing -> undefined;
      Owner ->
          case next_owner(State, Idx) of
            {_, Node, _} -> future_primary;
            _ -> {fallback, Owner}
          end
    end.

%% @doc Return details for a pending partition ownership change.
-spec next_owner(State :: chstate(),
                 Idx :: integer()) -> pending_change().

next_owner(State, Idx) ->
    case lists:keyfind(Idx, 1, State#chstate.next) of
      false -> {undefined, undefined, undefined};
      NInfo -> next_owner(NInfo)
    end.

%% @doc Return details for a pending partition ownership change.
-spec next_owner(State :: chstate(), Idx :: integer(),
                 Mod :: module()) -> pending_change().

next_owner(State, Idx, Mod) ->
    NInfo = lists:keyfind(Idx, 1, State#chstate.next),
    next_owner_status(NInfo, Mod).

%% @doc Retrieve status of the transfer from the next owner entry.
%% @param NInfo Entry of the next owner list.
%% @param Mod VNode module to be considered in the transfer.
%% @returns Pair of owner and next owner and the transfer status.
-spec next_owner_status(NInfo :: term() | false,
                        Mod :: atom()) -> pending_change().

%% TODO specify type for next owner entries.
next_owner_status(NInfo, Mod) ->
    case NInfo of
      false -> {undefined, undefined, undefined};
      {_, Owner, NextOwner, _Transfers, complete} ->
          {Owner, NextOwner, complete};
      {_, Owner, NextOwner, Transfers, _Status} ->
          case ordsets:is_element(Mod, Transfers) of
            true -> {Owner, NextOwner, complete};
            false -> {Owner, NextOwner, awaiting}
          end
    end.

%% @private
next_owner({_, Owner, NextOwner, _Transfers, Status}) ->
    {Owner, NextOwner, Status}.

%% @doc Compute a list of all indices for which the transfer is complete and
%%      also give the name of the owner and next owner.
%% @param Mod VNode module to be considered in the transfer.
%% @param State State to work on.
%% @returns List of tuples containing index, owner, and next owner for completed
%%          transfers.
-spec completed_next_owners(Mod :: module(),
                            State :: chstate()) -> [{integer(), term(),
                                                     term()}].

completed_next_owners(Mod, #chstate{next = Next}) ->
    [{Idx, O, NO}
     || NInfo = {Idx, _, _, _, _} <- Next,
        {O, NO, complete} <- [next_owner_status(NInfo, Mod)]].

%% @doc Returns true if all cluster members have seen the current ring.
-spec ring_ready(State :: chstate()) -> boolean().

ring_ready(State0) ->
    check_tainted(State0,
                  "Error: riak_core_ring/ring_ready called "
                  "on tainted ring"),
    Owner = owner_node(State0),
    State = update_seen(Owner, State0),
    Seen = State#chstate.seen,
    Members = get_members(State#chstate.members,
                          [valid, leaving, exiting]),
    VClock = State#chstate.vclock,
    R = [begin
           case orddict:find(Node, Seen) of
             error -> false;
             {ok, VC} -> vclock:equal(VClock, VC)
           end
         end
         || Node <- Members],
    Ready = lists:all(fun (X) -> X =:= true end, R),
    Ready.

%% @doc Like {@link ring_ready/1} with the local raw ring.
-spec ring_ready() -> boolean().

ring_ready() ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    ring_ready(Ring).

%% @doc Compute a list of all seen with an outdated vector clock.
%% @param State0 State to work on.
%% @returns List of all seen entries with an outdated vector clock.
-spec ring_ready_info(State0 :: chstate()) -> [{term(),
                                                vclock:vclock()}].

ring_ready_info(State0) ->
    Owner = owner_node(State0),
    State = update_seen(Owner, State0),
    Seen = State#chstate.seen,
    Members = get_members(State#chstate.members,
                          [valid, leaving, exiting]),
    RecentVC = orddict:fold(fun (_, VC, Recent) ->
                                    case vclock:descends(VC, Recent) of
                                      true -> VC;
                                      false -> Recent
                                    end
                            end,
                            State#chstate.vclock, Seen),
    Outdated = orddict:filter(fun (Node, VC) ->
                                      not vclock:equal(VC, RecentVC) and
                                        lists:member(Node, Members)
                              end,
                              Seen),
    Outdated.

%% @doc Marks a pending transfer as completed.
-spec handoff_complete(State :: chstate(),
                       Idx :: integer(), Mod :: module()) -> chstate().

handoff_complete(State, Idx, Mod) ->
    transfer_complete(State, Idx, Mod).

%% @doc Inform that the ring has changed and trigger updates.
%% @param Node Node for which the ring has changed.
%% @param State State to work on.
%% @return Updated state.
-spec ring_changed(Node :: term(),
                   State :: chstate()) -> chstate().

ring_changed(Node, State) ->
    check_tainted(State,
                  "Error: riak_core_ring/ring_changed called "
                  "on tainted ring"),
    internal_ring_changed(Node, State).

%% @doc Determine the future ring after all pending ownership transfers have
%%      been completed.
%% @param State State to work on.
%% @returns The future ring.
-spec future_ring(State :: chstate()) -> chstate().

future_ring(State) ->
    FutureState = change_owners(State,
                                all_next_owners(State)),
    %% Individual nodes will move themselves from leaving to exiting if they
    %% have no ring ownership, this is implemented in riak_core_ring_handler.
    %% Emulate it here to return similar ring.
    Leaving = get_members(FutureState#chstate.members,
                          [leaving]),
    FutureState2 = lists:foldl(fun (Node, StateAcc) ->
                                       case indices(StateAcc, Node) of
                                         [] ->
                                             riak_core_ring:exit_member(Node,
                                                                        StateAcc,
                                                                        Node);
                                         _ -> StateAcc
                                       end
                               end,
                               FutureState, Leaving),
    FutureState2#chstate{next = []}.

%% @doc Output the given ring in a readable format.
%% @param Ring The ring to be printed.
%% @param Opts Options determining the layout and contents of the output.
%% @returns `ok'.
-spec pretty_print(Ring :: chstate(),
                   Opts :: [term()]) -> ok.

pretty_print(Ring, Opts) ->
    OptNumeric = lists:member(numeric, Opts),
    OptLegend = lists:member(legend, Opts),
    Out = proplists:get_value(out, Opts, standard_io),
    TargetN = proplists:get_value(target_n, Opts,
                                  application:get_env(riak_core, target_n_val,
                                                      undefined)),
    Owners = riak_core_ring:all_members(Ring),
    Indices = riak_core_ring:all_owners(Ring),
    RingSize = length(Indices),
    Numeric = OptNumeric orelse length(Owners) > 26,
    case Numeric of
      true ->
          Ids = [integer_to_list(N)
                 || N <- lists:seq(1, length(Owners))];
      false ->
          Ids = [[Letter]
                 || Letter <- lists:seq(97, 96 + length(Owners))]
    end,
    Names = lists:zip(Owners, Ids),
    case OptLegend of
      true ->
          io:format(Out, "~36..=s Nodes ~36..=s~n", ["", ""]),
          _ = [begin
                 NodeIndices = [Idx
                                || {Idx, Owner} <- Indices, Owner =:= Node],
                 RingPercent = length(NodeIndices) * 100 / RingSize,
                 io:format(Out, "Node ~s: ~w (~5.1f%) ~s~n",
                           [Name, length(NodeIndices), RingPercent, Node])
               end
               || {Node, Name} <- Names],
          io:format(Out, "~36..=s Ring ~37..=s~n", ["", ""]);
      false -> ok
    end,
    case Numeric of
      true ->
          Ownership = [orddict:fetch(Owner, Names)
                       || {_Idx, Owner} <- Indices],
          io:format(Out, "~p~n", [Ownership]);
      false ->
          lists:foldl(fun ({_, Owner}, N) ->
                              Name = orddict:fetch(Owner, Names),
                              case N rem TargetN of
                                0 -> io:format(Out, "~s|", [[Name]]);
                                _ -> io:format(Out, "~s", [[Name]])
                              end,
                              N + 1
                      end,
                      1, Indices),
          io:format(Out, "~n", [])
    end.

%% @doc Return a ring with all transfers cancelled - for claim sim
cancel_transfers(Ring) -> Ring#chstate{next = []}.

%% @doc Get the weight associated with the given member.
%% @param Member Name of the member.
%% @param State Ring the node is a member of.
%% @returns The member's wieght or undefined.
-spec get_weight(Member :: term(),
                 State :: chstate()) -> pos_integer().

get_weight(Member, State) ->
    case get_member_meta(State, Member, weight) of
      undefined -> ?DEFAULT_WEIGHT;
      Weight -> Weight
    end.

%% @doc Get current mapping of node name to its weight.
%% @param Ring Ring to get the weights from.
-spec get_weights(Ring ::
                      chstate()) -> chash:owner_weight_list().

get_weights(State) ->
    [{Member, get_weight(Member, State)}
     || Member <- all_members(State)].

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
internal_ring_changed(Node, CState0) ->
    CState = update_seen(Node, CState0),
    case ring_ready(CState) of
      false -> CState;
      true -> riak_core_claimant:ring_changed(Node, CState)
    end.

%% @private
merge_meta({N1, M1}, {N2, M2}) ->
    Meta = dict:merge(fun (_, D1, D2) ->
                              pick_val({N1, D1}, {N2, D2})
                      end,
                      M1, M2),
    log_meta_merge(M1, M2, Meta),
    Meta.

%% @private
pick_val({N1, M1}, {N2, M2}) ->
    case {M1#meta_entry.lastmod, N1} >
           {M2#meta_entry.lastmod, N2}
        of
      true -> M1;
      false -> M2
    end.

%% @private
%% Log ring metadata input and result for debug purposes
log_meta_merge(M1, M2, Meta) ->
    logger:debug("Meta A: ~p", [M1]),
    logger:debug("Meta B: ~p", [M2]),
    logger:debug("Meta result: ~p", [Meta]).

%% @private
%% Log result of a ring reconcile. In the case of ring churn,
%% subsequent log messages will allow us to track ring versions.
%% Handle legacy rings as well.
log_ring_result(#chstate{vclock = V, members = Members,
                         next = Next}) ->
    logger:debug("Updated ring vclock: ~p, Members: ~p, "
                 "Next: ~p",
                 [V, Members, Next]).

%% @private
internal_reconcile(State, OtherState) ->
    VNode = owner_node(State),
    State2 = update_seen(VNode, State),
    OtherState2 = update_seen(VNode, OtherState),
    Seen = reconcile_seen(State2, OtherState2),
    State3 = State2#chstate{seen = Seen},
    OtherState3 = OtherState2#chstate{seen = Seen},
    SeenChanged = not equal_seen(State, State3),
    %% Try to reconcile based on vector clock, chosing the most recent state.
    VC1 = State3#chstate.vclock,
    VC2 = OtherState3#chstate.vclock,
    %% vclock:merge has different results depending on order of input vclocks
    %% when input vclocks have same counter but different timestamps. We need
    %% merge to be deterministic here, hence the additional logic.
    VMerge1 = vclock:merge([VC1, VC2]),
    VMerge2 = vclock:merge([VC2, VC1]),
    case {vclock:equal(VMerge1, VMerge2), VMerge1 < VMerge2}
        of
      {true, _} -> VC3 = VMerge1;
      {_, true} -> VC3 = VMerge1;
      {_, false} -> VC3 = VMerge2
    end,
    Newer = vclock:descends(VC1, VC2),
    Older = vclock:descends(VC2, VC1),
    Equal = equal_cstate(State3, OtherState3),
    case {Equal, Newer, Older} of
      {_, true, false} ->
          {SeenChanged, State3#chstate{vclock = VC3}};
      {_, false, true} ->
          {true,
           OtherState3#chstate{nodename = VNode, vclock = VC3}};
      {true, _, _} ->
          {SeenChanged, State3#chstate{vclock = VC3}};
      {_, true, true} ->
          %% Exceptional condition that should only occur during
          %% rolling upgrades and manual setting of the ring.
          %% Merge as a divergent case.
          State4 = reconcile_divergent(VNode, State3,
                                       OtherState3),
          {true, State4#chstate{nodename = VNode}};
      {_, false, false} ->
          %% Unable to reconcile based on vector clock, merge rings.
          State4 = reconcile_divergent(VNode, State3,
                                       OtherState3),
          {true, State4#chstate{nodename = VNode}}
    end.

%% @private
reconcile_divergent(VNode,
                    StateA = #chstate{claimant = Claimant1, rvsn = VC1},
                    StateB = #chstate{claimant = Claimant2, rvsn = VC2}) ->
    VClock = vclock:increment(VNode,
                              vclock:merge([StateA#chstate.vclock,
                                            StateB#chstate.vclock])),
    Members = reconcile_members(StateA, StateB),
    Meta = merge_meta({StateA#chstate.nodename,
                       StateA#chstate.meta},
                      {StateB#chstate.nodename, StateB#chstate.meta}),
    V1Newer = vclock:descends(VC1, VC2),
    V2Newer = vclock:descends(VC2, VC1),
    EqualVC = vclock:equal(VC1, VC2) and
                (Claimant1 =:= Claimant2),
    OldState = case {EqualVC, V1Newer, V2Newer} of
                 {true, _, _} -> StateA;
                 {_, true, false} -> StateA;
                 {_, false, true} -> StateB;
                 {_, _, _} ->
                     %% Ring versions were divergent, so fall back to reconciling based
                     %% on claimant. Under normal operation, divergent ring versions
                     %% should only occur if there are two different claimants, and one
                     %% claimant is invalid. For example, when a claimant is removed and
                     %% a new claimant has just taken over. We therefore chose the ring
                     %% with the valid claimant.
                     ValidMembers = [Member
                                     || {Member, {Status, _, _}} <- Members,
                                        Status =/= invalid],
                     CValid1 = lists:member(Claimant1, ValidMembers),
                     CValid2 = lists:member(Claimant2, ValidMembers),
                     case {CValid1, CValid2} of
                       {true, false} -> StateA;
                       {false, true} -> StateB;
                       {_, _} -> %
                           %% This can occur when removed/down nodes are still
                           %% up and gossip to each other. We need to pick a
                           %% claimant to handle this case, although the choice
                           %% is irrelevant as a correct valid claimant will
                           %% eventually emerge when the ring converges.
                           case Claimant1 < Claimant2 of
                             true -> StateA;
                             false -> StateB
                           end
                     end
               end,
    State = OldState#chstate{members = Members,
                             meta = Meta},
    NewState = reconcile_ring(State),
    NewState1 = NewState#chstate{vclock = VClock,
                                 members = Members, meta = Meta},
    log_ring_result(NewState1),
    NewState1.

%% @private
%% @doc Merge two members list using status vector clocks when possible,
%%      and falling back to manual merge for divergent cases.
reconcile_members(StateA, StateB) ->
    orddict:merge(fun (_K, {Valid1, VC1, Meta1},
                       {Valid2, VC2, Meta2}) ->
                          New1 = vclock:descends(VC1, VC2),
                          New2 = vclock:descends(VC2, VC1),
                          MergeVC = vclock:merge([VC1, VC2]),
                          case {New1, New2} of
                            {true, false} ->
                                MergeMeta = lists:ukeysort(1, Meta1 ++ Meta2),
                                {Valid1, MergeVC, MergeMeta};
                            {false, true} ->
                                MergeMeta = lists:ukeysort(1, Meta2 ++ Meta1),
                                {Valid2, MergeVC, MergeMeta};
                            {_, _} ->
                                MergeMeta = lists:ukeysort(1, Meta1 ++ Meta2),
                                {merge_status(Valid1, Valid2), MergeVC,
                                 MergeMeta}
                          end
                  end,
                  StateA#chstate.members, StateB#chstate.members).

%% @private
reconcile_seen(StateA, StateB) ->
    orddict:merge(fun (_, VC1, VC2) ->
                          vclock:merge([VC1, VC2])
                  end,
                  StateA#chstate.seen, StateB#chstate.seen).

%% @private
reconcile_ring(StateA) ->
    ClaimingMembers = claiming_members(StateA),
    Weights = [{Node, Weight}
               || {Node, Weight} <- riak_core_ring:get_weights(StateA),
                  lists:member(Node, ClaimingMembers)],
    FutureState = FutureState = change_owners(StateA,
                                              all_next_owners(StateA)),
    FutureCHash = riak_core_ring:chash(FutureState),
    OldCHash = chash(StateA),
    NewCHash = chash:change(FutureCHash, Weights),
    {OldHOCHash, NewHOCHash} =
        chash:make_handoff_chash(OldCHash, NewCHash),
    DiffList = chash:diff_list(OldHOCHash, NewHOCHash),
    Next2 = lists:ukeysort(1,
                           [{Index, Owner, NextOwner, [], awaiting}
                            || {Index, Owner, NextOwner} <- DiffList]),
    StateA#chstate{next = Next2, chring = OldHOCHash}.

%% @private
merge_status(invalid, _) -> invalid;
merge_status(_, invalid) -> invalid;
merge_status(down, _) -> down;
merge_status(_, down) -> down;
merge_status(joining, _) -> joining;
merge_status(_, joining) -> joining;
merge_status(valid, _) -> valid;
merge_status(_, valid) -> valid;
merge_status(exiting, _) -> exiting;
merge_status(_, exiting) -> exiting;
merge_status(leaving, _) -> leaving;
merge_status(_, leaving) -> leaving;
merge_status(_, _) -> invalid.

%% @private
transfer_complete(CState = #chstate{next = Next,
                                    vclock = VClock},
                  Idx, Mod) ->
    {Idx, Owner, NextOwner, Transfers, Status} =
        lists:keyfind(Idx, 1, Next),
    Transfers2 = ordsets:add_element(Mod, Transfers),
    VNodeMods = ordsets:from_list([VMod
                                   || {_, VMod} <- riak_core:vnode_modules()]),
    Status2 = case {Status, Transfers2} of
                {complete, _} -> complete;
                {awaiting, VNodeMods} -> complete;
                _ -> awaiting
              end,
    Next2 = lists:keyreplace(Idx, 1, Next,
                             {Idx, Owner, NextOwner, Transfers2, Status2}),
    VClock2 = vclock:increment(Owner, VClock),
    CState#chstate{next = Next2, vclock = VClock2}.

%% @private
get_members(Members) ->
    get_members(Members,
                [joining, valid, leaving, exiting, down]).

%% @private
get_members(Members, Types) ->
    [Node
     || {Node, {V, _, _}} <- Members,
        lists:member(V, Types)].

%% @private
update_seen(Node,
            CState = #chstate{vclock = VClock, seen = Seen}) ->
    Seen2 = orddict:update(Node,
                           fun (SeenVC) -> vclock:merge([SeenVC, VClock]) end,
                           VClock, Seen),
    CState#chstate{seen = Seen2}.

%% @private
equal_cstate(StateA, StateB) ->
    equal_cstate(StateA, StateB, false).

equal_cstate(StateA, StateB, false) ->
    T1 = equal_members(StateA#chstate.members,
                       StateB#chstate.members),
    T2 = vclock:equal(StateA#chstate.rvsn,
                      StateB#chstate.rvsn),
    T3 = equal_seen(StateA, StateB),
    T4 = equal_rings(StateA, StateB),
    %% Clear fields checked manually and test remaining through equality.
    %% Note: We do not consider cluster name in equality.
    StateA2 = StateA#chstate{nodename = undefined,
                             members = undefined, vclock = undefined,
                             rvsn = undefined, seen = undefined,
                             chring = undefined, meta = undefined,
                             clustername = undefined},
    StateB2 = StateB#chstate{nodename = undefined,
                             members = undefined, vclock = undefined,
                             rvsn = undefined, seen = undefined,
                             chring = undefined, meta = undefined,
                             clustername = undefined},
    T5 = StateA2 =:= StateB2,
    T1 andalso T2 andalso T3 andalso T4 andalso T5.

%% @private
equal_members(M1, M2) ->
    L = orddict:merge(fun (_, {Status1, VC1, Meta1},
                           {Status2, VC2, Meta2}) ->
                              Status1 =:= Status2 andalso
                                vclock:equal(VC1, VC2) andalso Meta1 =:= Meta2
                      end,
                      M1, M2),
    {_, R} = lists:unzip(L),
    lists:all(fun (X) -> X =:= true end, R).

%% @private
equal_seen(StateA, StateB) ->
    Seen1 = filtered_seen(StateA),
    Seen2 = filtered_seen(StateB),
    L = orddict:merge(fun (_, VC1, VC2) ->
                              vclock:equal(VC1, VC2)
                      end,
                      Seen1, Seen2),
    {_, R} = lists:unzip(L),
    lists:all(fun (X) -> X =:= true end, R).

%% @private
filtered_seen(State = #chstate{seen = Seen}) ->
    case get_members(State#chstate.members) of
      [] -> Seen;
      Members ->
          orddict:filter(fun (N, _) -> lists:member(N, Members)
                         end,
                         Seen)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

sequence_test() ->
    I1 = 0,
    I2 = 730750818665451459101842416358141509827966271488,
    A = fresh(a, [a, dummy1]),
    B1 = A#chstate{nodename = b},
    B2 = transfer_node(I1, b, B1),
    ?assertEqual(B2, (transfer_node(I1, b, B2))),
    {no_change, A1} = reconcile(B1, A),
    C1 = A#chstate{nodename = c},
    C2 = transfer_node(I1, c, C1),
    {new_ring, A2} = reconcile(C2, A1),
    {new_ring, A3} = reconcile(B2, A2),
    C3 = transfer_node(I2, c, C2),
    {new_ring, C4} = reconcile(A3, C3),
    {new_ring, A4} = reconcile(C4, A3),
    {new_ring, B3} = reconcile(A4, B2),
    ?assertEqual((A4#chstate.chring), (B3#chstate.chring)),
    ?assertEqual((B3#chstate.chring), (C4#chstate.chring)).

param_fresh_test() ->
    application:set_env(riak_core, ring_creation_size, 4),
    ?assert((equal_cstate(fresh(), fresh(node())))),
    ?assertEqual((owner_node(fresh())), (node())).

index_test() ->
    Ring0 = fresh(node()),
    Ring1 = transfer_node(0, x, Ring0),
    ?assertEqual(0, (random_other_index(Ring0))),
    ?assertEqual(0, (random_other_index(Ring1))),
    ?assertEqual((node()), (index_owner(Ring0, 0))),
    ?assertEqual(x, (index_owner(Ring1, 0))),
    ?assertEqual((lists:sort([x, node()])),
                 (lists:sort(diff_nodes(Ring0, Ring1)))).

reconcile_test() ->
    Ring0 = fresh(node()),
    Ring1 = transfer_node(0, x, Ring0),
    %% Only members and seen should have changed
    {new_ring, Ring2} = reconcile(fresh(someone_else),
                                  Ring1),
    ?assertNot((equal_cstate(Ring1, Ring2, false))),
    RingB0 = fresh(node()),
    RingB1 = transfer_node(0, x, RingB0),
    RingB2 = RingB1#chstate{nodename = b},
    ?assertMatch({no_change, _},
                 (reconcile(Ring1, RingB2))),
    {no_change, RingB3} = reconcile(Ring1, RingB2),
    ?assert((equal_cstate(RingB2, RingB3))).

metadata_inequality_test() ->
    Ring0 = fresh(node()),
    Ring1 = update_meta(key, val, Ring0),
    ?assertNot((equal_rings(Ring0, Ring1))),
    ?assertEqual((Ring1#chstate.meta),
                 (merge_meta({node0, Ring0#chstate.meta},
                             {node1, Ring1#chstate.meta}))),
    timer:sleep(1001), % ensure that lastmod is at least a second later
    Ring2 = update_meta(key, val2, Ring1),
    ?assertEqual((get_meta(key, Ring2)),
                 (get_meta(key,
                           #chstate{meta =
                                        merge_meta({node1, Ring1#chstate.meta},
                                                   {node2,
                                                    Ring2#chstate.meta})}))),
    ?assertEqual((get_meta(key, Ring2)),
                 (get_meta(key,
                           #chstate{meta =
                                        merge_meta({node2, Ring2#chstate.meta},
                                                   {node1,
                                                    Ring1#chstate.meta})}))).

metadata_remove_test() ->
    Ring0 = fresh(node()),
    ?assert((equal_rings(Ring0, remove_meta(key, Ring0)))),
    Ring1 = update_meta(key, val, Ring0),
    timer:sleep(1001), % ensure that lastmod is at least one second later
    Ring2 = remove_meta(key, Ring1),
    ?assertEqual(undefined, (get_meta(key, Ring2))),
    ?assertEqual(undefined,
                 (get_meta(key,
                           #chstate{meta =
                                        merge_meta({node1, Ring1#chstate.meta},
                                                   {node2,
                                                    Ring2#chstate.meta})}))),
    ?assertEqual(undefined,
                 (get_meta(key,
                           #chstate{meta =
                                        merge_meta({node2, Ring2#chstate.meta},
                                                   {node1,
                                                    Ring1#chstate.meta})}))).

rename_test() ->
    Ring0 = fresh(node()),
    Ring = rename_node(Ring0, node(), new@new),
    ?assertEqual(new@new, (owner_node(Ring))),
    ?assertEqual([new@new], (all_members(Ring))).

exclusion_test() ->
    I = 730750818665451459101842416358141509827966271488,
    Ring1 = fresh(node(), [node(), x]),
    ?assertEqual(0, (random_other_index(Ring1, [I]))),
    ?assertEqual(no_indices,
                 (random_other_index(Ring1, [0]))),
    ?assertEqual([{I, node()}, {0, x}],
                 (preflist(hash:as_binary(1), Ring1))).

random_other_node_test() ->
    Ring0 = fresh(node()),
    ?assertEqual(no_node, (random_other_node(Ring0))),
    Ring1 = add_member(node(), Ring0, new@new),
    Ring2 = transfer_node(0, new@new, Ring1),
    ?assertEqual(new@new, (random_other_node(Ring2))).

membership_test() ->
    RingA1 = fresh(nodeA),
    ?assertEqual([nodeA], (all_members(RingA1))),
    RingA2 = add_member(nodeA, RingA1, nodeB),
    RingA3 = add_member(nodeA, RingA2, nodeC),
    ?assertEqual([nodeA, nodeB, nodeC],
                 (all_members(RingA3))),
    RingA4 = remove_member(nodeA, RingA3, nodeC),
    ?assertEqual([nodeA, nodeB], (all_members(RingA4))),
    %% Node should stay removed
    {_, RingA5} = reconcile(RingA3, RingA4),
    ?assertEqual([nodeA, nodeB], (all_members(RingA5))),
    %% Add node in parallel, check node stays removed
    RingB1 = add_member(nodeB, RingA3, nodeC),
    {_, RingA6} = reconcile(RingB1, RingA5),
    ?assertEqual([nodeA, nodeB], (all_members(RingA6))),
    %% Add node as parallel descendent, check node is added
    RingB2 = add_member(nodeB, RingA6, nodeC),
    {_, RingA7} = reconcile(RingB2, RingA6),
    ?assertEqual([nodeA, nodeB, nodeC],
                 (all_members(RingA7))),
    Priority = [{invalid, 1}, {down, 2}, {joining, 3},
                {valid, 4}, {exiting, 5}, {leaving, 6}],
    RingX1 = fresh(nodeA),
    RingX2 = add_member(nodeA, RingX1, nodeB),
    RingX3 = add_member(nodeA, RingX2, nodeC),
    ?assertEqual(joining, (member_status(RingX3, nodeC))),
    %% Parallel/sibling status changes merge based on priority
    [begin
       RingT1 = set_member(nodeA, RingX3, nodeC, StatusA),
       ?assertEqual(StatusA, (member_status(RingT1, nodeC))),
       RingT2 = set_member(nodeB, RingX3, nodeC, StatusB),
       ?assertEqual(StatusB, (member_status(RingT2, nodeC))),
       StatusC = case PriorityA < PriorityB of
                   true -> StatusA;
                   false -> StatusB
                 end,
       {_, RingT3} = reconcile(RingT2, RingT1),
       ?assertEqual(StatusC, (member_status(RingT3, nodeC)))
     end
     || {StatusA, PriorityA} <- Priority,
        {StatusB, PriorityB} <- Priority],
    %% Related status changes merge to descendant
    [begin
       RingT1 = set_member(nodeA, RingX3, nodeC, StatusA),
       ?assertEqual(StatusA, (member_status(RingT1, nodeC))),
       RingT2 = set_member(nodeB, RingT1, nodeC, StatusB),
       ?assertEqual(StatusB, (member_status(RingT2, nodeC))),
       RingT3 = set_member(nodeA, RingT1, nodeA, valid),
       {_, RingT4} = reconcile(RingT2, RingT3),
       ?assertEqual(StatusB, (member_status(RingT4, nodeC)))
     end
     || {StatusA, _} <- Priority, {StatusB, _} <- Priority],
    ok.

ring_version_test() ->
    Ring1 = fresh(nodeA),
    Ring2 = add_member(node(), Ring1, nodeA),
    Ring3 = add_member(node(), Ring2, nodeB),
    ?assertEqual(nodeA, (claimant(Ring3))),
    #chstate{rvsn = RVsn, vclock = VClock} = Ring3,
    RingA1 = transfer_node(0, nodeA, Ring3),
    RingA2 = RingA1#chstate{vclock =
                                vclock:increment(nodeA, VClock)},
    RingB1 = transfer_node(0, nodeB, Ring3),
    RingB2 = RingB1#chstate{vclock =
                                vclock:increment(nodeB, VClock)},
    %% RingA1 has most recent ring version
    {_, RingT1} = reconcile(RingA2#chstate{rvsn =
                                               vclock:increment(nodeA, RVsn)},
                            RingB2),
    ?assertEqual(nodeA, (index_owner(RingT1, 0))),
    %% RingB1 has most recent ring version
    {_, RingT2} = reconcile(RingA2,
                            RingB2#chstate{rvsn =
                                               vclock:increment(nodeB, RVsn)}),
    ?assertEqual(nodeB, (index_owner(RingT2, 0))),
    %% Divergent ring versions, merge based on claimant
    {_, RingT3} = reconcile(RingA2#chstate{rvsn =
                                               vclock:increment(nodeA, RVsn)},
                            RingB2#chstate{rvsn =
                                               vclock:increment(nodeB, RVsn)}),
    ?assertEqual(nodeA, (index_owner(RingT3, 0))),
    %% Divergent ring versions, one valid claimant. Merge on claimant.
    RingA3 = RingA2#chstate{claimant = nodeA},
    RingA4 = remove_member(nodeA, RingA3, nodeB),
    RingB3 = RingB2#chstate{claimant = nodeB},
    RingB4 = remove_member(nodeB, RingB3, nodeA),
    {_, RingT4} = reconcile(RingA4#chstate{rvsn =
                                               vclock:increment(nodeA, RVsn)},
                            RingB3#chstate{rvsn =
                                               vclock:increment(nodeB, RVsn)}),
    ?assertEqual(nodeA, (index_owner(RingT4, 0))),
    {_, RingT5} = reconcile(RingA3#chstate{rvsn =
                                               vclock:increment(nodeA, RVsn)},
                            RingB4#chstate{rvsn =
                                               vclock:increment(nodeB, RVsn)}),
    ?assertEqual(nodeB, (index_owner(RingT5, 0))).

-endif.

%% -------------------------------------------------------------------
%%
%% taken from: https://github.com/basho/riak_core/blob/develop/src/chashbin.erl
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(chashbin).

-export([create/1, to_chash/1, to_list/1,
         to_list_filter/2, responsible_index/2,
         responsible_position/2, index_owner/2,
         num_partitions/1]).

-export([iterator/2, exact_iterator/2, itr_value/1,
         itr_pop/2, itr_next/1, itr_next_while/2]).

-export_type([chashbin/0]).

%% 160 bits for hash, 16 bits for node id
%% these macros break edoc
%% also these macros are not used consistently, commenting out for now
%%-define(UNIT, 176).
%%-define(ENTRY, binary-unit:?UNIT).

-type owners_bin() :: <<_:_*176>>.

%% 64 bits for wieht, 16 bits for node id
-type weights_bin() :: <<_:_*80>>.

-type index() :: chash:index_as_int().

-type pred_fun() :: fun(({index(),
                          node()}) -> boolean()).

-type chash_key() :: index() | chash:index().

-ifndef(namespaced_types).

-record(chashbin,
        {size  :: pos_integer(), owners  :: owners_bin(),
         weights  :: weights_bin(),
         nodes  :: erlang:tuple(node())}).

-else.

-record(chashbin,
        {size  :: pos_integer(), owners  :: owners_bin(),
         weights  :: weights_bin(),
         nodes  :: erlang:tuple(node())}).

-endif.

-type chashbin() :: #chashbin{}.

-record(iterator,
        {pos  :: non_neg_integer(), start  :: non_neg_integer(),
         chbin  :: chashbin()}).

-type iterator() :: #iterator{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a `chashbin' from the provided `chash'
-spec create(chash:chash()) -> chashbin().

create(CHash) ->
    Nodes2 = chash:members(CHash),
    Nodes3 = lists:zip(Nodes2,
                       lists:seq(1, length(Nodes2))),
    OBin = owner_bin(chash:nodes(CHash), Nodes3, <<>>),
    WBin = weight_bin(chash:weights(CHash), Nodes3, <<>>),
    #chashbin{size = chash:size(CHash), owners = OBin,
              weights = WBin, nodes = list_to_tuple(Nodes2)}.

%% @doc Convert a `chashbin' back to a `chash'
-spec to_chash(chashbin()) -> chash:chash().

to_chash(CHBin) ->
    {L, W} = to_list(CHBin), {L, {stale, {}}, W}.

%% @doc Convert a `chashbin' to a list of `{Owner, Index}' pairs and a list of
%% `{Owner, Weight}' pairs.
-spec to_list(chashbin()) -> {[chash:node_entry()],
                              [chash:owner_weight()]}.

to_list(#chashbin{owners = OBin, weights = WBin,
                  nodes = Nodes}) ->
    {[{element(Id, Nodes), chash:int_to_index(Idx)}
      || <<Idx:160/integer, Id:16/integer>> <= OBin],
     [{element(Id, Nodes), Weight}
      || <<Weight:64/integer, Id:16/integer>> <= WBin]}.

%% @doc
%% Convert a `chashbin' to a list of `{Index, Owner}' pairs for
%% which `Pred({Index, Owner})' returns `true'
-spec to_list_filter(pred_fun(),
                     chashbin()) -> [{index(), node()}].

to_list_filter(Pred,
               #chashbin{owners = Bin, nodes = Nodes}) ->
    [{Idx, element(Id, Nodes)}
     || <<Idx:160/integer, Id:16/integer>> <= Bin,
        Pred({Idx, element(Id, Nodes)})].

%% @doc Determine the ring index responsible for a given chash key
-spec responsible_index(chash_key(),
                        chashbin()) -> index().

responsible_index(<<HashKey:160/integer>>, CHBin) ->
    responsible_index(HashKey, CHBin);
responsible_index(HashKey, #chashbin{owners = Bin}) ->
    {Res, true} = lists:foldl(fun (I, {Start, Done}) ->
                                      case Done of
                                        true -> {Start, true};
                                        false ->
                                            End = Start + I,
                                            case (HashKey >= Start) and
                                                   (HashKey < End)
                                                of
                                              true -> {Start, true};
                                              false -> {End, false}
                                            end
                                      end
                              end,
                              {0, false},
                              [Idx
                               || <<Idx:160/integer, _:16/integer>> <= Bin]),
    Res.

%% @doc Determine the ring position responsible for a given chash key
-spec responsible_position(chash_key(),
                           chashbin()) -> non_neg_integer().

responsible_position(<<HashKey:160/integer>>, CHBin) ->
    responsible_position(HashKey, CHBin);
responsible_position(HashKey,
                     #chashbin{owners = Bin}) ->
    {Res, _, true} = lists:foldl(fun (I,
                                      {Pos, Start, Done}) ->
                                         case Done of
                                           true -> {Pos, Start, true};
                                           false ->
                                               End = Start + I,
                                               case (HashKey >= Start) and
                                                      (HashKey < End)
                                                   of
                                                 true -> {Pos, Start, true};
                                                 false -> {Pos + 1, End, false}
                                               end
                                         end
                                 end,
                                 {1, 0, false},
                                 [Idx
                                  || <<Idx:160/integer, _:16/integer>> <= Bin]),
    Res.

%% @doc Return the node that owns the given index
-spec index_owner(index(), chashbin()) -> node().

index_owner(Idx, CHBin) ->
    case itr_value(exact_iterator(Idx, CHBin)) of
      {Owner, _Index} -> Owner;
      _ ->
          %% Match the behavior for riak_core_ring:index_owner/2
          exit({badmatch, false})
    end.

%% @doc Return the number of partitions in a given `chashbin'
-spec num_partitions(chashbin()) -> pos_integer().

num_partitions(#chashbin{size = Size}) -> Size.

%% ===================================================================
%% Public Iterator API
%% ===================================================================

%% @doc
%% Return an iterator pointing to the index responsible for the given chash key
-spec iterator(first | chash_key(),
               chashbin()) -> iterator().

iterator(first, CHBin) ->
    #iterator{pos = 0, start = 0, chbin = CHBin};
iterator(<<HashKey:160/integer>>, CHBin) ->
    iterator(HashKey, CHBin);
iterator(HashKey, CHBin) ->
    Pos = responsible_position(HashKey, CHBin),
    #iterator{pos = Pos, start = Pos, chbin = CHBin}.

%% @doc Return the `{Index, Owner}' pair pointed to by the iterator
-spec itr_value(iterator()) -> {index(), node()}.

itr_value(#iterator{pos = Pos,
                    chbin = #chashbin{owners = Bin, nodes = Nodes}}) ->
    <<_:Pos/binary-unit:176, Idx:160/integer, Id:16/integer,
      _/binary>> =
        Bin,
    Owner = element(Id, Nodes),
    {Idx, Owner}.

%% @doc Advance the iterator by one ring position
-spec itr_next(iterator()) -> iterator() | done.

itr_next(Itr = #iterator{pos = Pos, start = Start,
                         chbin = CHBin}) ->
    Pos2 = (Pos + 1) rem CHBin#chashbin.size,
    case Pos2 of
      Start -> done;
      _ -> Itr#iterator{pos = Pos2}
    end.

%% @doc
%% Advance the iterator `N' times, returning a list of the traversed
%% `{Index, Owner}' pairs as well as the new iterator state
-spec itr_pop(pos_integer(), iterator()) -> {[{index(),
                                               node()}],
                                             iterator()}.

itr_pop(N, Itr = #iterator{pos = Pos, chbin = CHBin}) ->
    #chashbin{size = Size, owners = Bin, nodes = Nodes} =
        CHBin,
    L = case Bin of
          <<_:Pos/binary-unit:176, Bin2:N/binary-unit:176,
            _/binary>> ->
              [{Idx, element(Id, Nodes)}
               || <<Idx:160/integer, Id:16/integer>> <= Bin2];
          _ ->
              Left = N + Pos - Size,
              Skip = Pos - Left,
              <<Bin3:Left/binary-unit:176, _:Skip/binary-unit:176,
                Bin2/binary>> =
                  Bin,
              L1 = [{Idx, element(Id, Nodes)}
                    || <<Idx:160/integer, Id:16/integer>> <= Bin2],
              L2 = [{Idx, element(Id, Nodes)}
                    || <<Idx:160/integer, Id:16/integer>> <= Bin3],
              L1 ++ L2
        end,
    Pos2 = (Pos + N) rem Size,
    Itr2 = Itr#iterator{pos = Pos2},
    {L, Itr2}.

%% @doc Advance the iterator while `Pred({Index, Owner})' returns `true'
-spec itr_next_while(pred_fun(),
                     iterator()) -> iterator().

itr_next_while(Pred, Itr) ->
    case Pred(itr_value(Itr)) of
      false -> Itr;
      true -> itr_next_while(Pred, itr_next(Itr))
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% Convert list of {Owner, Index} pairs into `chashbin' binary representation
-spec owner_bin([chash:node_entry()],
                [{node(), pos_integer()}], binary()) -> owners_bin().

owner_bin([], _, Bin) -> Bin;
owner_bin([{Owner, Idx} | Owners], Nodes, Bin) ->
    {Owner, Id} = lists:keyfind(Owner, 1, Nodes),
    Bin2 = <<Bin/binary,
             (chash:index_to_int(Idx)):160/integer, Id:16/integer>>,
    owner_bin(Owners, Nodes, Bin2).

%% Convert list of {Owner, Weight} pairs into `chashbin' binary representation
-spec weight_bin([chash:owner_weight()],
                 [{node(), pos_integer()}], binary()) -> weights_bin().

weight_bin([], _, Bin) -> Bin;
weight_bin([{Owner, Weight} | Weights], Nodes, Bin) ->
    {Owner, Id} = lists:keyfind(Owner, 1, Nodes),
    Bin2 = <<Bin/binary, Weight:64/integer, Id:16/integer>>,
    owner_bin(Weights, Nodes, Bin2).

%% Return iterator pointing to the given index
exact_iterator(<<Idx:160/integer>>, CHBin) ->
    exact_iterator(Idx, CHBin);
exact_iterator(Idx, CHBin) ->
    Pos = responsible_position(Idx, CHBin),
    #iterator{pos = Pos, start = Pos, chbin = CHBin}.

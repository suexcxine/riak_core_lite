%% @doc The hash module abstracts a hash function together with its attributes
%% like maximum range and a library to convert the hash value to different types
%% and ranges. The hash function used is determined by the configuration
%% 'riak_core:hash'.
-module(hash).

-type algorithm() :: sha1.

%% Used to emphasize that the float value should be within [0.0, 1.0).
-type unit() :: float().

-define(HASH,
        application:getenv(riak_core, hash, sha1)).

-export([hash/1, as_integer/1, as_binary/1, as_unit/1,
         max_integer/0]).

%% @doc Hash the given key with the configured hash algorithm.
-spec hash(Key :: binary()) -> Value :: binary().

hash(Key) -> hash(?HASH, Key).

%% @doc Convert a value from its unit interval or binary representation to an
%% integer representation.
-spec as_integer(Value :: binary() |
                          unit()) -> integer().

as_integer(Value) -> as_integer(?HASH, Value).

%% @doc Convert a value from its unit interval or integer representation to a
%% binary representation.
-spec as_binary(Value :: integer() |
                         unit()) -> binary().

as_binary(Value) -> as_binary(?HASH, Value).

%% @doc Convert a value from its integer or binary representation to a
%% unit interval representation.
-spec as_unit(Value :: integer() | binary()) -> unit().

as_unit(Value) -> as_unit(?HASH, Value).

%% @doc Max value of the hash algorithm output.
-spec max_integer() -> integer().

max_integer() -> max_integer(?HASH).

%% ===================================================================
%% Implementations
%% ===================================================================

%% @see hash/1
-spec hash(Type :: algorithm(),
           Key :: binary()) -> Value :: binary().

hash(sha1, Key) -> crypto:hash(sha1, Key).

%% @see as_integer/1
-spec as_integer(Type :: algorithm(),
                 Value :: binary() | unit()) -> integer().

as_integer(sha1, Value) when is_binary(Value) ->
    <<Int:160/integer>> = Value, Int;
as_integer(sha1, Value) ->
    % WARN possible loss of precision.
    round(Value * max_integer()).

%% @see as_binary/1
-spec as_binary(Type :: algorithm(),
                Value :: integer() | unit()) -> binary().

as_binary(sha1, Value) when is_integer(Value) ->
    <<Value:160/integer>>;
as_binary(sha1, Value) ->
    as_binary(sha1, as_integer(Value)).

-spec as_unit(Type :: algorithm(),
              Value :: integer() | binary()) -> unit().

as_unit(sha1, Value) when is_integer(Value) ->
    Value / max_integer();
as_unit(sha1, Value) ->
    as_unit(sha1, as_integer(Value)).

%% @see max_integer/0
-spec max_integer(Type :: algorithm()) -> integer().

max_integer(sha1) -> 1 bsl 20 * 8.

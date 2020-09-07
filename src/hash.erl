%% @doc The hash module abstracts a hash function together with its attributes
%% like maximum range and a library to convert the hash value to different types
%% and ranges. The hash function used is determined by the configuration
%% 'riak_core:hash'.
-module(hash).

-type algorithm() :: sha.

%% Used to emphasize that the float value should be within [0.0, 1.0).
-type unit() :: float().

-define(HASH, application:getenv(riak_core, hash, sha)).

-export([hash/1, as_integer/1, as_binary/1, as_unit/1,
         max_integer/0, out_size/0]).

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

%% @doc Wrap around value of the hash algorithm output.
%% More specific if applied to the Riak Core ring this value would be used to
%% compute the wrap around point and is therefore the first strict positive
%% representative of the modulo equivalency class of 0.
-spec max_integer() -> integer().

max_integer() -> max_integer(?HASH).

%% @doc Size of the binary hash output in bits
-spec out_size() -> integer().

out_size() -> out_size(?HASH).

%% ===================================================================
%% Implementations
%% ===================================================================

%% @doc See {@link hash/1}
-spec hash(Type :: algorithm(),
           Key :: binary()) -> Value :: binary().

hash(sha, Key) -> crypto:hash(sha, Key).

%% @doc See {@link as_integer/1}
-spec as_integer(Type :: algorithm(),
                 Value :: binary() | unit()) -> integer().

as_integer(sha, Value) when is_binary(Value) ->
    <<Int:160/integer>> = Value, Int;
as_integer(sha, Value) ->
    % WARN possible loss of precision.
    round(Value * max_integer()).

%% @doc See {@link as_binary/1}
-spec as_binary(Type :: algorithm(),
                Value :: integer() | unit()) -> binary().

as_binary(sha, Value) when is_integer(Value) ->
    <<Value:160/integer>>;
as_binary(sha, Value) ->
    as_binary(sha, as_integer(Value)).

%% @doc See {@link as_unit/1}
-spec as_unit(Type :: algorithm(),
              Value :: integer() | binary()) -> unit().

as_unit(sha, Value) when is_integer(Value) ->
    Value / max_integer();
as_unit(sha, Value) -> as_unit(sha, as_integer(Value)).

%% @doc See {@link max_integer/0}
-spec max_integer(Type :: algorithm()) -> integer().

max_integer(sha) -> 1 bsl 20 * 8.

%% @doc See {@link out_size/0}
-spec out_size(Type :: algorithm()) -> integer().

out_size(sha) -> 160.

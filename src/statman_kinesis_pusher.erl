-module(statman_kinesis_pusher).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {timer, prefix, stream, key, filtermapper}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Interval = application:get_env(statman_kinesis, interval, 60000),
    Prefix = application:get_env(statman_kinesis, prefix, undefined),
    Key = application:get_env(statman_kinesis, key, undefined),
    {ok, Stream} = application:get_env(statman_kinesis, stream),
    Filtermapper = case application:get_env(statman_kinesis, filtermapper) of
                       {ok, {M, F}} ->
                           fun M:F/1;
                       {ok, Fun} when is_function(Fun) ->
                           Fun;
                       undefined ->
                           fun default_filtermapper/1
                   end,
    Timer = erlang:start_timer(Interval, self(), {push, Interval}),
    {ok, #state{timer = Timer,
                prefix = Prefix,
                stream = Stream,
                key = Key,
                filtermapper = Filtermapper}}.


handle_call(get_timer, _From, State) ->
    {reply, {ok, State#state.timer}, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({timeout, Timer, {push, Interval}}, #state{timer = Timer, prefix = Prefix, stream = Stream, key = Key} = State) ->
    NewTimer = erlang:start_timer(Interval, self(), {push, Interval}),
    {ok, Metrics} = statman_aggregator:get_window(Interval div 1000),
    Filtered = lists:filtermap(State#state.filtermapper, Metrics),
    BinaryMetrics = list_to_binary(serialize_metrics(Prefix, Filtered)),
    BinaryStream = list_to_binary(Stream),

    Payload = [{<<"Data">>, base64:encode(BinaryMetrics)},
               {<<"PartitionKey">>, base64:encode(binary_key(Key))},
               {<<"StreamName">>, BinaryStream}],

    try kinetic:put_record(Payload) of
      _ -> ok
    catch
      _:_ -> error_logger:info_msg("statman_kinesis: record could not be saved. ~n")
    end,
    
    {noreply, State#state{timer = NewTimer}};


handle_info(Info, State) ->
    error_logger:info_msg("statman_kinesis: got unexpected message: ~p~n", [Info]),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

default_filtermapper(Metric) ->
    case application:get_env(statman_kinesis, whitelist) of
        {ok, List} ->
            lists:member(proplists:get_value(key, Metric), List);
        undefined ->
            %% Send all metrics if a whitelist is not configured
            true
    end.

serialize_metrics(Prefix, Metrics) ->
    lists:flatmap(fun (Metric) ->
                          format_metric(Prefix, Metric)
                  end, Metrics).

format_metric(Prefix, Metric) ->
    case proplists:get_value(type, Metric) of
        counter ->
            [<<(binary_prefix(Prefix))/binary, (format_counter(Metric))/binary, $\n>>];
        gauge ->
            [<<(binary_prefix(Prefix))/binary, (format_gauge(Metric))/binary, $\n>>];
        histogram ->
            [<<(binary_prefix(Prefix))/binary, Data/binary, $\n>>
             || Data <- format_histogram(Metric)];
        _ ->
            []
    end.

binary_prefix(undefined) ->
    <<>>;
binary_prefix(B) when is_binary(B) ->
    <<B/binary, $.>>.

binary_key(undefined) ->
    crypto:rand_bytes(4);
binary_key(B) when is_binary(B) ->
    B.


format_counter(Metric) ->
    Name = format_key(proplists:get_value(key, Metric)),
    Value = number_to_binary(proplists:get_value(value, Metric)),
    Timestamp = number_to_binary(timestamp()),
    <<Name/binary, " ", Value/binary, " ", Timestamp/binary>>.

format_gauge(Metric) ->
    Name = format_key(proplists:get_value(key, Metric)),
    Value = number_to_binary(proplists:get_value(value, Metric)),
    Timestamp = number_to_binary(timestamp()),
    <<Name/binary, " ", Value/binary, " ", Timestamp/binary>>.

format_histogram(Metric) ->
    Summary = statman_histogram:summary(proplists:get_value(value, Metric)),
    Percentiles = [min, p25, mean, median, p75, p95, p99, p999, max],
    lists:flatmap(
      fun (Percentile) ->
              Name = format_key({proplists:get_value(key, Metric), Percentile}),
              case proplists:get_value(Percentile, Summary) of
                  N when is_number(N) ->
                      Timestamp = number_to_binary(timestamp()),
                      [<<Name/binary, " ", (number_to_binary(N))/binary, " ",
                        Timestamp/binary>>];
                  _ ->
                      []
              end
      end, Percentiles).


format_key(Key) ->
    sanitize(iolist_to_binary(format_key2(Key))).

format_key2(Key) when is_integer(Key) ->
    [integer_to_list(Key)];
format_key2(Key) when is_tuple(Key) ->
    [string:join(lists:map(fun format_key2/1, tuple_to_list(Key)), ".")];
format_key2(Key) when is_atom(Key) ->
    [atom_to_list(Key)];
format_key2(Key) when is_binary(Key) orelse is_list(Key) ->
    [Key].

sanitize(Key) ->
    binary:replace(binary:replace(Key, <<" ">>, <<"_">>, [global]),
                   <<"/">>, <<".">>, [global]).

number_to_binary(N) when is_float(N) ->
    iolist_to_binary(io_lib:format("~f", [N]));
number_to_binary(N) when is_integer(N) ->
    list_to_binary(integer_to_list(N)).


timestamp() ->
    {MegaSeconds, Seconds, _} = os:timestamp(),
    MegaSeconds * 1000000 + Seconds.
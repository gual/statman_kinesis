# Statman -> Kinesis

This library pushes your metrics collected with [statman](https://github.com/knutin/statman) to a Kinesis stream using [kinetic](https://github.com/tsloughter/kinetic).

For statman histograms only the percentiles are pushed as data points.

It is based on [statman_graphite](https://github.com/chrisavl/statman_graphite).

## Using it

* You need to have Kinetic configured.
* You need to set the `stream` application variable before starting
the app. After that you just need to record some metrics with statman.

```erlang
application:set_env(statman_kinesis, stream, "my-cool-stream").
application:start(statman_kinesis).
statman_aggregator:start_link().
record_some_statman_stats().
```

You can use the `prefix` application variable to set a global prefix that will
be prepended to all metrics before they are send to Kinesis. 

The `key` application variable can also be set, and will be used as partition key 
for the stream, if it's not set then a random 4-byte key will be used every time.

```erlang
application:set_env(statman_kinesis, prefix, <<"my-api-key">>).
application:set_env(statman_kinesis, prefix, <<"my-api-key.", (list_to_binary(atom_to_list(node())))/binary>>).
```

### Filtering/rewriting

There are two ways to filter what statman metrics are sent to kinesis. By
default all metrics are sent. The easy way to filter is to define the
`whitelist` application variable, it should be a list of statman keys that you
want to send to Kinesis.

```erlang
application:set_env(statman_kinesis, whitelist, [foo, {bar, baz}]).
```

If you want to do dynamic filtering and/or rewrite the metrics before sending
them you can set the `filtermapper` application variable, it should be a fun
that matches the docs for `lists:filtermap`. This option precedes the
`whitelist` option.

```erlang
application:set_env(statman_kinesis, filtermapper, {mymodule, myfunction}).
%% Or as a fun
application:set_env(statman_kinesis, filtermapper,
                    fun (Metric) ->
                         case proplists:get_value(key, Metric) of
                              foo ->
                                  false;
                              bar ->
                                  {true, lists:keystore(key, 1, Metric, {key, baz})};
                              baz ->
                                  true
                          end
                    end).
```

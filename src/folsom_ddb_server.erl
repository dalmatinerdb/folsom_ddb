%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 19 Aug 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(folsom_ddb_server).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-ignore_xref([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          host :: undefined,
          port :: undefined | pos_integer(),
          bucket :: binary(),
          ref :: reference(),
          ddb = undefined,
          interval = 1000 :: pos_integer(),
          vm_metrics = [] :: list(),
          prefix = [<<"folsom">>]:: [binary()]
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, DDBrror}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    case application:get_env(folsom_ddb, endpoint) of
        {ok, {Host, Port}} ->
            {ok, BucketS} = application:get_env(folsom_ddb, bucket),
            Bucket = list_to_binary(BucketS),
            {ok, PrefixS} = application:get_env(folsom_ddb, prefix),
            Prefix = list_to_binary(PrefixS),
            {ok, Interval} = application:get_env(folsom_ddb, interval),
            VMMetrics = case application:get_env(folsom_ddb, vm_metrics) of
                            {ok, true} ->
                                [];
                            _ ->
                                []
                        end,
            {ok, #state{interval = Interval, prefix = [Prefix], bucket = Bucket,
                        vm_metrics = VMMetrics, host = Host, port = Port},
             0};
        _ ->
            {ok, #state{}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({timeout, _R, tick},
            #state{ref = _R, interval = FlushInterval, ddb = DDB,
                   vm_metrics = VMSpec, prefix = Prefix}
            = State) ->
    Time = timestamp(),

    DDB1 = ddb_reply(ddb_tcp:batch_start(Time, DDB)),
    DDB2 = do_vm_metrics(Prefix, VMSpec, DDB1),
    Spec = folsom_metrics:get_metrics_info(),
    DDB3 = do_metrics(Prefix, Spec, DDB2),
    DDB4 = ddb_reply(ddb_tcp:batch_end(DDB3)),
    Ref = erlang:start_timer(FlushInterval, self(), tick),
    {noreply, State#state{ref = Ref, ddb = DDB4}};

handle_info(timeout, State = #state{host = Host, port = Port, bucket = Bucket,
                                    interval = Interval}) ->
    {ok, DDB} = ddb_tcp:connect(Host, Port),
    DDB1 = case ddb_tcp:stream_mode(Bucket, Interval div 1000, DDB) of
               {ok, DDBx} ->
                   DDBx;
               {error, _, DDBx} ->
                   DDBx
           end,
    Ref = erlang:start_timer(Interval, self(), tick),
    {noreply, State#state{ddb = DDB1, ref = Ref}};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{ddb = undefined}) ->
    ok;
terminate(_Reason, #state{ddb = DDB}) ->
    ddb_tcp:close(DDB),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, DDBxtra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _DDBxtra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_vm_metrics(_Prefix, [], DDB) ->
    DDB;

do_vm_metrics(Prefix, [_|Spec], DDB) ->
    do_vm_metrics(Prefix, Spec, DDB).


do_metrics(Prefix, [{N, [{type, histogram} | _]} | Spec], DDB) ->
    Prefix1 = [Prefix, metric_name(N)],
    Hist = folsom_metrics:get_histogram_statistics(N),
    DDB1 = build_histogram(Hist, Prefix1, DDB),
    do_metrics(Prefix, Spec, DDB1);

do_metrics(Prefix, [{N, [{type, spiral} | _]} | Spec], DDB) ->
    [{count, Count}, {one, One}] = folsom_metrics:get_metric_value(N),
    DDB1 = send([{[Prefix, metric_name(N), <<"count">>], Count},
                 {[Prefix, metric_name(N), <<"one">>], One}], DDB),
    do_metrics(Prefix, Spec, DDB1);

do_metrics(Prefix,
           [{N, [{type, counter} | _]} | Spec], DDB) ->
    Count = folsom_metrics:get_metric_value(N),
    DDB1 = send([Prefix, metric_name(N)], Count, DDB),
    do_metrics(Prefix, Spec, DDB1);

do_metrics(Prefix,
           [{N, [{type, gauge} | _]} | Spec], DDB) ->
    Count = folsom_metrics:get_metric_value(N),
    DDB1 = send([Prefix, metric_name(N)], Count, DDB),
    do_metrics(Prefix, Spec, DDB1);

do_metrics(Prefix,
           [{N, [{type, duration} | _]} | Spec], DDB) ->
    K = [Prefix, metric_name(N)],
    DDB1 = build_histogram(folsom_metrics:get_metric_value(N),
                           K, DDB),
    do_metrics(Prefix, Spec, DDB1);

do_metrics(Prefix,
           [{N, [{type, meter} | _]} | Spec], DDB) ->
    Prefix1 = [Prefix, metric_name(N)],
    Scale = 1000*1000,
    [{count, Count},
     {one, One},
     {five, Five},
     {fifteen, Fifteen},
     {day, Day},
     {mean, Mean},
     {acceleration,
      [{one_to_five, OneToFive},
       {five_to_fifteen, FiveToFifteen},
       {one_to_fifteen, OneToFifteen}]}]
        = folsom_metrics:get_metric_value(N),
    DDB1 = send([{[Prefix1, <<"count">>], Count},
                 {[Prefix1, <<"one">>], round(One*Scale)},
                 {[Prefix1, <<"five">>], round(Five*Scale)},
                 {[Prefix1, <<"fifteen">>], round(Fifteen*Scale)},
                 {[Prefix1, <<"day">>], round(Day*Scale)},
                 {[Prefix1, <<"mean">>], round(Mean*Scale)},
                 {[Prefix1, <<"one_to_five">>], round(OneToFive*Scale)},
                 {[Prefix1, <<"five_to_fifteen">>], round(FiveToFifteen*Scale)},
                 {[Prefix1, <<"one_to_fifteen">>], round(OneToFifteen*Scale)}],
                DDB),
    do_metrics(Prefix, Spec, DDB1);

do_metrics(Prefix,
           [{N, [{type, meter_reader} | _]} | Spec], DDB) ->
    Prefix1 = [Prefix, metric_name(N)],
    Scale = 1000*1000,
    [{one, One},
     {five, Five},
     {fifteen, Fifteen},
     {mean, Mean},
     {acceleration,
      [{one_to_five, OneToFive},
       {five_to_fifteen, FiveToFifteen},
       {one_to_fifteen, OneToFifteen}]}]
        = folsom_metrics:get_metric_value(N),
    DDB1 = send([{[Prefix1, <<"one">>], round(One*Scale)},
                 {[Prefix1, <<"five">>], round(Five*Scale)},
                 {[Prefix1, <<"fifteen">>], round(Fifteen*Scale)},
                 {[Prefix1, <<"mean">>], round(Mean*Scale)},
                 {[Prefix1, <<"one_to_five">>], round(OneToFive*Scale)},
                 {[Prefix1, <<"five_to_fifteen">>], round(FiveToFifteen*Scale)},
                 {[Prefix1, <<"one_to_fifteen">>], round(OneToFifteen*Scale)}],
                DDB),
    do_metrics(Prefix, Spec, DDB1);

do_metrics(_Prefix, [], DDB) ->
    DDB.

build_histogram([], _, DDB) ->
    DDB;

build_histogram([{min, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"min">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{max, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"max">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{arithmetic_mean, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"arithmetic_mean">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{geometric_mean, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"geometric_mean">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{harmonic_mean, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"harmonic_mean">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{median, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"median">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{variance, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"variance">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{standard_deviation, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"standard_deviation">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{skewness, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"skewness">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{kurtosis, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"kurtosis">>], round(V), DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([{percentile,
                  [{50, P50}, {75, P75}, {90, P90}, {95, P95}, {99, P99},
                   {999, P999}]} | H], Pfx, DDB) ->
    DDB1 = send([{[Pfx, <<"p50">>], round(P50)},
                 {[Pfx, <<"p75">>], round(P75)},
                 {[Pfx, <<"p90">>], round(P90)},
                 {[Pfx, <<"p95">>], round(P95)},
                 {[Pfx, <<"p99">>], round(P99)},
                 {[Pfx, <<"p999">>], round(P999)}], DDB),
    build_histogram(H, Pfx, DDB1);

build_histogram([{n, V} | H], Prefix, DDB) ->
    DDB1 = send([Prefix, <<"count">>], V, DDB),
    build_histogram(H, Prefix, DDB1);

build_histogram([_ | H], Prefix, DDB) ->
    build_histogram(H, Prefix, DDB).

metric_name(B) when is_binary(B) ->
    B;
metric_name(L) when is_list(L) ->
    erlang:list_to_binary(L);
metric_name(N1) when
      is_atom(N1) ->
    a2b(N1);
metric_name({N1, N2}) when
      is_atom(N1), is_atom(N2) ->
    [a2b(N1), a2b(N2)];
metric_name({N1, N2, N3}) when
      is_atom(N1), is_atom(N2), is_atom(N3) ->
    [a2b(N1), a2b(N2), a2b(N3)];
metric_name({N1, N2, N3, N4}) when
      is_atom(N1), is_atom(N2), is_atom(N3), is_atom(N4) ->
    [a2b(N1), a2b(N2), a2b(N3), a2b(N4)];
metric_name(T) when is_tuple(T) ->
    [metric_name(DDB) || DDB <- tuple_to_list(T)].

a2b(A) ->
    erlang:atom_to_binary(A, utf8).

timestamp() ->
    erlang:system_time(seconds).

send(MVs, DDB) ->
    MVs1 = [{lists:flatten(Metric), mmath_bin:from_list([Value])}
           || {Metric, Value} <- MVs],
    ddb_reply(ddb_tcp:batch(MVs1, DDB)).

send(Metric, Value, DDB) when is_integer(Value) ->
    Metric1 = lists:flatten(Metric),
    ddb_reply(ddb_tcp:batch(Metric1, mmath_bin:from_list([Value]), DDB)).

ddb_reply({ok, DDB}) ->
    DDB;
ddb_reply({error, _, DDB}) ->
    DDB.


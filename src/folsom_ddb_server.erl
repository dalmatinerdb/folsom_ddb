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
          ref :: reference(),
          ddb = dunefined,
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
    case application:get_env(folsom_ddb, endpoint) of
        {ok, {Host, Port}} ->
            process_flag(trap_exit, true),
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
            {ok, DDB} = ddb_tcp:connect(Host, Port),
            {ok, DDB1} = ddb_tcp:stream_mode(Bucket, Interval div 1000, DDB),
            Ref = erlang:start_timer(Interval, self(), tick),
            {ok, #state{ref = Ref, interval = Interval, prefix = [Prefix],
                        vm_metrics = VMMetrics, ddb = DDB1}};
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
    DDB1 = do_vm_metrics(Prefix, Time, VMSpec, DDB),
    Spec = folsom_metrics:get_metrics_info(),
    DDB2 = do_metrics(Prefix, Time, Spec, DDB1),
    Ref = erlang:start_timer(FlushInterval, self(), tick),
    {noreply, State#state{ref = Ref, ddb = DDB2}};

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

do_vm_metrics(_Prefix, _Time, [], DDB) ->
    DDB;

do_vm_metrics(Prefix, Time, [_|Spec], DDB) ->
    do_vm_metrics(Prefix, Time, Spec, DDB).


do_metrics(Prefix, Time, [{N, [{type, histogram}]} | Spec], DDB) ->
    Prefix1 = [Prefix, metric_name(N)],
    Hist = folsom_metrics:get_histogram_statistics(N),
    DDB1 = build_histogram(Hist, Prefix1, Time, DDB),
    do_metrics(Prefix, Time, Spec, DDB1);

do_metrics(Prefix, Time, [{N, [{type, spiral}]} | Spec], DDB) ->
    [{count, Count}, {one, One}] = folsom_metrics:get_metric_value(N),
    DDB1 = send([Prefix, metric_name(N), <<"count">>], Time, Count, DDB),
    DDB2 = send([Prefix, metric_name(N), <<"one">>], Time, One, DDB1),
    do_metrics(Prefix, Time, Spec, DDB2);

do_metrics(Prefix, Time,
           [{N, [{type, counter}]} | Spec], DDB) ->
    Count = folsom_metrics:get_metric_value(N),
    DDB1 = send([Prefix, metric_name(N)], Time, Count, DDB),
    do_metrics(Prefix, Time, Spec, DDB1);

do_metrics(Prefix, Time,
           [{N, [{type, counter}]} | Spec], DDB) ->
    Count = folsom_metrics:get_metric_value(N),
    DDB1 = send([Prefix, metric_name(N)], Time, Count, DDB),
    do_metrics(Prefix, Time, Spec, DDB1);

do_metrics(Prefix, Time,
           [{N, [{type, duration}]} | Spec], DDB) ->
    K = [Prefix, metric_name(N)],
    DDB1 = build_histogram(folsom_metrics:get_metric_value(N),
                           K, Time, DDB),
    do_metrics(Prefix, Time, Spec, DDB1);

do_metrics(Prefix, Time,
           [{N, [{type, meter}]} | Spec], DDB) ->
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
    DDB1 = send([Prefix1, <<"count">>], Time, Count, DDB),
    DDB2 = send([Prefix1, <<"one">>], Time, round(One*Scale), DDB1),
    DDB3 = send([Prefix1, <<"five">>], Time, round(Five*Scale), DDB2),
    DDB4 = send([Prefix1, <<"fifteen">>], Time, round(Fifteen*Scale), DDB3),
    DDB5 = send([Prefix1, <<"day">>], Time, round(Day*Scale), DDB4),
    DDB6 = send([Prefix1, <<"mean">>], Time, round(Mean*Scale), DDB5),
    DDB7 = send([Prefix1, <<"one_to_five">>], Time,
                round(OneToFive*Scale), DDB6),
    DDB8 = send([Prefix1, <<"five_to_fifteen">>],
                Time, round(FiveToFifteen*Scale), DDB7),
    DDB9 = send([Prefix1, <<"one_to_fifteen">>],
                Time, round(OneToFifteen*Scale), DDB8),
    do_metrics(Prefix, Time, Spec, DDB9);

do_metrics(_Prefix, _Time, [], DDB) ->
    DDB.

build_histogram([], _, _, DDB) ->
    DDB;

build_histogram([{min, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"min">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{max, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"max">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{arithmetic_mean, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"arithmetic_mean">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{geometric_mean, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"geometric_mean">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{harmonic_mean, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"harmonic_mean">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{median, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"median">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{variance, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"variance">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{standard_deviation, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"standard_deviation">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{skewness, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"skewness">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{kurtosis, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"kurtosis">>], Time, round(V), DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([{percentile,
                  [{50, P50}, {75, P75}, {90, P90}, {95, P95}, {99, P99},
                   {999, P999}]} | H], Pfx, Time, DDB) ->
    DDB1 = send([Pfx, <<"p50">>], Time, round(P50), DDB),
    DDB2 = send([Pfx, <<"p75">>], Time, round(P75), DDB1),
    DDB3 = send([Pfx, <<"p90">>], Time, round(P90), DDB2),
    DDB4 = send([Pfx, <<"p95">>], Time, round(P95), DDB3),
    DDB5 = send([Pfx, <<"p99">>], Time, round(P99), DDB4),
    DDB6 = send([Pfx, <<"p999">>], Time, round(P999), DDB5),
    build_histogram(H, Pfx, Time, DDB6);

build_histogram([{n, V} | H], Prefix, Time, DDB) ->
    DDB1 = send([Prefix, <<"count">>], Time, V, DDB),
    build_histogram(H, Prefix, Time, DDB1);

build_histogram([_ | H], Prefix, Time, DDB) ->
    build_histogram(H, Prefix, Time, DDB).

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
    {Meg, S, _} = os:timestamp(),
    Meg*1000000 + S.


send(Metric, Time, Value, DDB) when is_integer(Value) ->
    Metric1 = dproto:metric_from_list(lists:flatten(Metric)),
    {ok, DDB1} = ddb_tcp:send(Metric1, Time, mmath_bin:from_list([Value]), DDB),
    DDB1.

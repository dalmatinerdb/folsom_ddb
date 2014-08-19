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

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(EP(K, T, V), dproto_udp:encode_points(K, T, V)).

-record(state, {
          ref :: reference(),
          socket :: gen_udp:socket(),
          host :: inet:ip_address() | inet:hostname(),
          port :: inet:port_number(),
          header :: binary(),
          interval :: pos_integer(),
          vm_metrics :: boolean(),
          buffer_size :: pos_integer(),
          prefix :: binary()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
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
    {ok, Bucket} = application:get_env(folsom_ddb, bucket),
    {ok, {Host, Port}} = application:get_env(folsom_ddb, endpoint),
    {ok, Interval} = application:get_env(folsom_ddb, interval),
    {ok, BufferSize} = application:get_env(folsom_ddb, buffer_size),
    VMMetrics = case application:get_env(folsom_ddb, vm_metrics) of
                    {ok, true} ->
                        [];
                    _ ->
                        []
                end,
    {ok, PfxS} = application:get_env(folsom_ddb, prefix),
    Header = dproto_udp:encode_header(list_to_binary(Bucket)),
    Prefix = <<(list_to_binary(PfxS))/binary, ".",
               (erlang:atom_to_binary(node(), utf8))/binary >>,
    Ref = case application:get_env(folsom_ddb, enabled) of
              {ok, true} ->
                  erlang:start_timer(Interval, self(), tick);
              _ ->
                  undefined
          end,
    {ok, #state{ref = Ref, host = Host, port = Port, interval = Interval,
                buffer_size = BufferSize, header = Header, prefix = Prefix,
                vm_metrics = VMMetrics}}.

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
            #state{ref = _R, interval = FlushInterval, socket = Socket,
                   host = Host, port = Port, buffer_size = MaxSize,
                   vm_metrics = VMSpec, header = Header, prefix = Prefix}
            = State) ->
    Time = timestamp(),
    Acc = do_vm_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time,
                        VMSpec, Header),
    Spec = folsom_metrics:get_metrics_info(),
    case do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec,
                    Acc) of
        <<>> ->
            ok;
        Acc1 ->
            ok = gen_udp:send(Socket, Host, Port, Acc1)
    end,
    Ref = erlang:start_timer(FlushInterval, self(), tick),
    {noreply, State#state{ref = Ref}};

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
terminate(_Reason, #state{socket = S}) ->
    gen_udp:close(S),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_vm_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec, Acc)
  when byte_size(Acc) >= MaxSize ->
    ok = gen_udp:send(Socket, Host, Port, Acc),
    do_vm_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec,
                  Header);

do_vm_metrics(_Socket, _Host, _Port, _Header, _Prefix, _Time, _MaxSize, [],
              Acc) ->
    Acc.

do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec, Acc)
  when byte_size(Acc) >= MaxSize ->
    ok = gen_udp:send(Socket, Host, Port, Acc),
    do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec, Header);

do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time,
           [{N, [{type, histogram}]} | Spec], Acc) ->
    K = <<Prefix/binary, ".", (metric_name(N))/binary>>,
    Acc1 = build_histogram(folsom_metrics:get_histogram_statistics(N),
                           K, Time, Acc),
    do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec, Acc1);

do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time,
           [{N, [{type, spiral}]} | Spec], Acc) ->
    [{count, Count}, {one, One}] = folsom_metrics:get_metric_value(N),
    K = <<Prefix/binary, ".", (metric_name(N))/binary>>,
    E1 = ?EP(<<K/binary, ".count">>, Time, Count),
    E2 = ?EP(<<K/binary, ".one">>, Time, One),
    do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec,
               <<Acc/binary, E1/binary, E2/binary>>);

do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time,
           [{N, [{type, counter}]} | Spec], Acc) ->
    K = <<Prefix/binary, ".", (metric_name(N))/binary>>,
    Count = folsom_metrics:get_metric_value(N),
    E = ?EP(K, Time, Count),
    do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec,
               <<Acc/binary, E/binary>>);

do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time,
           [{N, [{type, duration}]} | Spec], Acc) ->
    K = <<Prefix/binary, ".", (metric_name(N))/binary>>,
    Acc1 = build_histogram(folsom_metrics:get_histogram_statistics(N),
                           K, Time, Acc),
    do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec, Acc1);

do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time,
           [{N, [{type, meter}]} | Spec], Acc) ->
    K = <<Prefix/binary, ".", (metric_name(N))/binary>>,
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
    E1 = ?EP(<<K/binary, ".count">>, Time, Count),
    E2 = ?EP(<<K/binary, ".one">>, Time, round(One*Scale)),
    E3 = ?EP(<<K/binary, ".five">>, Time, round(Five*Scale)),
    E4 = ?EP(<<K/binary, ".fifteen">>, Time, round(Fifteen*Scale)),
    E5 = ?EP(<<K/binary, ".day">>, Time, round(Day*Scale)),
    E6 = ?EP(<<K/binary, ".mean">>, Time, round(Mean*Scale)),
    E7 = ?EP(<<K/binary, ".one_to_five">>, Time, round(OneToFive*Scale)),
    E8 = ?EP(<<K/binary, ".five_to_fifteen">>, Time, round(FiveToFifteen*Scale)),
    E9 = ?EP(<<K/binary, ".one_to_fifteen">>, Time, round(OneToFifteen*Scale)),
    Acc1 = <<Acc/binary, E1/binary, E2/binary, E3/binary, E4/binary, E5/binary,
             E6/binary, E6/binary, E7/binary, E8/binary, E9/binary>>,
    do_metrics(Socket, Host, Port, Header, Prefix, MaxSize, Time, Spec, Acc1);

do_metrics(_Socket, _Host, _Port, _Header, _Prefix, _MaxSize, _Time, [], Acc) ->
    Acc.

build_histogram([], _, _, Acc) ->
    Acc;

build_histogram([{min, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".min">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{max, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".max">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{arithmetic_mean, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".arithmetic_mean">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{geometric_mean, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".geometric_mean">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{harmonic_mean, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".harmonic_mean">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{median, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".median">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{variance, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".variance">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{standard_deviation, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".standard_deviation">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{skewness, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".skewness">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{kurtosis, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".kurtosis">>, Time, round(V)),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([{percentile,
                  [{50, P50}, {75, P75}, {90, P90}, {95, P95}, {99, P99},
                   {999, P999}]} | H], Pfx, Time, Acc) ->
    E1 = ?EP(<<Pfx/binary, ".p50">>, Time, round(P50)),
    E2 = ?EP(<<Pfx/binary, ".p75">>, Time, round(P75)),
    E3 = ?EP(<<Pfx/binary, ".p90">>, Time, round(P90)),
    E4 = ?EP(<<Pfx/binary, ".p95">>, Time, round(P95)),
    E5 = ?EP(<<Pfx/binary, ".p99">>, Time, round(P99)),
    E6 = ?EP(<<Pfx/binary, ".p999">>, Time, round(P999)),
    build_histogram(H, Pfx, Time, <<Acc, E1/binary, E2/binary, E3/binary,
                                    E4/binary, E5/binary, E6/binary>>);

build_histogram([{n, V} | H], Prefix, Time, Acc) ->
    E = ?EP(<<Prefix/binary, ".count">>, Time, V),
    build_histogram(H, Prefix, Time, <<Acc, E/binary>>);

build_histogram([_ | H], Prefix, Time, Acc) ->
    build_histogram(H, Prefix, Time, Acc).

metric_name(N1) when is_atom(N1) ->
    erlang:binary_to_atom(N1, utf8);
metric_name({N1, N2}) when is_atom(N1), is_atom(N2) ->
    <<(erlang:atom_to_binary(N1, utf8))/binary, ".",
      (erlang:atom_to_binary(N2, utf8))/binary>>;
metric_name({N1, N2, N3}) when is_atom(N1), is_atom(N2), is_atom(N3) ->
    <<(erlang:atom_to_binary(N1, utf8))/binary, ".",
      (erlang:atom_to_binary(N2, utf8))/binary, ".",
      (erlang:atom_to_binary(N3, utf8))/binary>>.

timestamp() ->
    {Meg, S, _} = os:timestamp(),
    Meg*1000000 + S.

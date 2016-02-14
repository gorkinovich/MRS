%|==========================================================================================|
%| Copyright (c) 2016 Gorka Suárez García                                                   |
%|                                                                                          |
%| Permission is hereby granted, free of charge, to any person obtaining a copy             |
%| of this software and associated documentation files (the "Software"), to deal            |
%| in the Software without restriction, including without limitation the rights             |
%| to use, copy, modify, merge, publish, distribute, sublicense, and/or sell                |
%| copies of the Software, and to permit persons to whom the Software is                    |
%| furnished to do so, subject to the following conditions:                                 |
%|                                                                                          |
%| The above copyright notice and this permission notice shall be included in               |
%| all copies or substantial portions of the Software.                                      |
%|                                                                                          |
%| THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR               |
%| IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,                 |
%| FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE              |
%| AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER                   |
%| LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING                  |
%| FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER                      |
%| DEALINGS IN THE SOFTWARE.                                                                |
%|==========================================================================================|
-module(mapredsrv).
-author("Gorka Suárez García").
-behaviour(gen_server).
-export([
    start/2, send_map_reduce/2,
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

%%===========================================================================================
%% Data
%%===========================================================================================

-define(SERVER, ?MODULE).
-record(state, {workers = [], tasks = []}).

%%===========================================================================================
%% Interface
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Starts the server.
%% <br/><b>PlannerNode:</b> The node of the planner server.
%% @spec
%% start(Info::[any()], N::number()) ->
%%      {ok, Pid :: pid()} | ignore | {error, Reason :: term()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec start(Info::[any()], N::number()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()} .
start(Info, N) when is_list(Info), is_number(N), N > 0 ->
    gen_server:start({local, ?SERVER}, ?MODULE, {Info, N}, []).

send_map_reduce(Fmap, Freduce) when is_function(Fmap), is_function(Freduce) ->
    gen_server:cast(?SERVER, {mapreduce, self(), Fmap, Freduce}).

%%===========================================================================================
%% Utility
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Makes a partition with a given list.
%% <br/><b>Info:</b> The list to partition.
%% <br/><b>N:</b> The number of partitions.
%% @spec
%% partition(Info::[any()], N::number()) -> [[any()]]
%% @end
%%-------------------------------------------------------------------------------------------
-spec partition(Info::[any()], N::number()) -> [[any()]].
partition(Info, N) ->
    Size = length(Info) / N,
    ToIdx = fun(X, Y) -> round(1 + (Size * (X - 1)) + Y) end,
    Idxs = [{ToIdx(I, 0), ToIdx(I, Size)} || I <- lists:seq(1, N)],
    [lists:sublist(Info, A, B - A) || {A, B} <- Idxs].

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Adds an entry to a table.
%% <br/><b>TID:</b> The table to change.
%% <br/><b>Value:</b> The value to add.
%% @spec
%% add_entry(TID::atom(), Value::any()) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec add_entry(TID::atom(), Value::any()) -> any().
add_entry(TID, VS) when is_list(VS) ->
    [add_entry(TID, V) || V <- VS];
add_entry(TID, {K, V}) ->
    case ets:lookup(TID, K) of
        [{K, VS}|_] -> ets:insert(TID, {K, [V | VS]});
        _ -> ets:insert(TID, {K, [V]})
    end;
add_entry(_, _) ->
    ok.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Adds only one entry to a table.
%% <br/><b>TID:</b> The table to change.
%% <br/><b>Value:</b> The value to add.
%% @spec
%% add_one_entry(TID::atom(), Value::any()) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec add_one_entry(TID::atom(), Value::any()) -> any().
add_one_entry(TID, {K, V}) ->
    case ets:lookup(TID, K) of
        [{K, _}|_] -> ok;
        _ -> ets:insert(TID, {K, V})
    end;
add_one_entry(_, _) ->
    ok.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Launchs a map task to the workers.
%% <br/><b>Workers:</b> A list of the workers.
%% <br/><b>FMap:</b> The map function.
%% @spec
%% launch_map_task(Workers::[any()], FMap::fun((any()) -> any())) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec launch_map_task(Workers::[any()], FMap::fun((any()) -> any())) -> any().
launch_map_task(Workers, Fmap) ->
    TID = ets:new(mrs_util:ref_to_atom(make_ref()), [set, public]),
    spawn_link(
        fun() ->
            [mapredwrk:send_start_map(WID, Fmap) || {_, WID, _} <- Workers],
            generic_task(Workers, TID, mapend, fun add_entry/2)
        end
    ).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Launchs a reduce task to the workers.
%% <br/><b>Workers:</b> A list of the workers.
%% <br/><b>Freduce:</b> The reduce function.
%% <br/><b>MTID:</b> The map data result.
%% @spec
%% launch_reduce_task(Workers::[any()], Freduce::fun((any()) -> any()),
%%      MTID::atom()) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec launch_reduce_task(Workers::[any()], Freduce::fun((any()) -> any()),
    MTID::atom()) -> any().
launch_reduce_task(Workers, Freduce, MTID) ->
    TID = ets:new(mrs_util:ref_to_atom(make_ref()), [set, public]),
    spawn_link(
        fun() ->
            N = length(Workers),
            MILS = ets:foldl(fun(E, AS)-> [E | AS] end, [], MTID),
            WIDS = [WID || {_, WID, _} <- Workers],
            [mapredwrk:send_start_reduce(WID, Freduce, MInfo) ||
                {WID, MInfo} <- lists:zip(WIDS, partition(MILS, N))],
            generic_task(Workers, TID, reduceend, fun add_one_entry/2)
        end
    ).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Process the generic part of the task launch operations.
%% <br/><b>Workers:</b> A list of the workers.
%% <br/><b>TID:</b> The ID of the table with the result.
%% <br/><b>CMD:</b> The command to send.
%% <br/><b>AddEntry:</b> A function to add an entry value to the result.
%% @spec
%% generic_task(Workers::[any()], TID::atom(), CMD::atom(),
%%      AddEntry::fun((atom(), tuple()) -> any())) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec generic_task(Workers::[any()], TID::atom(), CMD::atom(),
    AddEntry::fun((atom(), tuple()) -> any())) -> any().
generic_task(Workers, TID, CMD, AddEntry) ->
    Receiver =
        fun Loop(Start, End) ->
            case Start < End of
                true ->
                    receive
                        {K, V} ->
                            AddEntry(TID, {K, V}),
                            Loop(Start, End);
                        {'end'} ->
                            Loop(Start + 1, End);
                        Value ->
                            AddEntry(TID, Value),
                            Loop(Start, End)
                    end;
                _ ->
                    ok
            end,
            ok
        end,
    Receiver(0, length(Workers)),
    gen_server:cast(?SERVER, {CMD, self(), TID}).

%%===========================================================================================
%% Callbacks
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @spec
%% init(Args :: term()) -> {ok, State :: #state{}} |
%%      {ok, State :: #state{}, timeout() | hibernate} |
%%      {stop, Reason :: term()} | ignore
%% @end
%%-------------------------------------------------------------------------------------------
-spec init(Args :: term()) -> ignore | {ok, State :: #state{}} |
    {ok, State :: #state{}, timeout() | hibernate} | {stop, Reason :: term()}.
init({Info, N}) when is_list(Info), is_number(N), N > 0 ->
    PID = self(),
    SubInfo = lists:zip(lists:seq(1, N), partition(Info, N)),
    StartWorker =
        fun(Idx, Inf) ->
            {ok, WID} = mapredwrk:start_link(Idx, PID, Inf),
            WID
        end,
    Workers = [{Idx, StartWorker(Idx, Inf), Inf} || {Idx, Inf} <- SubInfo],
    {ok, #state{workers = Workers}}.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Handles the call messages
%% @spec
%% handle_call(Request :: term(), From :: {pid(), Tag :: term()},
%%      State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}} |
%%      {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
%%      {noreply, NewState :: #state{}} |
%%      {noreply, NewState :: #state{}, timeout() | hibernate} |
%%      {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
%%      {stop, Reason :: term(), NewState :: #state{}}
%% @end
%%-------------------------------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Handles the cast messages.
%% @spec
%% handle_cast(Request :: term(), State :: #state{}) ->
%%      {noreply, NewState :: #state{}} |
%%      {noreply, NewState :: #state{}, timeout() | hibernate} |
%%      {stop, Reason :: term(), NewState :: #state{}}
%% @end
%%-------------------------------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast({mapreduce, Parent, Fmap, Freduce}, State) when is_pid(Parent),
    is_function(Fmap), is_function(Freduce) ->
    PID = launch_map_task(State#state.workers, Fmap),
    Tasks = [{PID, Parent, Freduce} | State#state.tasks],
    {noreply, State#state{tasks = Tasks}};
handle_cast({mapend, From, TID}, State) when is_pid(From) ->
    io:format("Map End => ~w~n", [From]),
    case lists:keyfind(From, 1, State#state.tasks) of
        {From, Parent, Freduce} ->
            PID = launch_reduce_task(State#state.workers, Freduce, TID),
            Tasks = [{PID, Parent} | lists:keydelete(From, 1, State#state.tasks)],
            {noreply, State#state{tasks = Tasks}};
        _ ->
            {noreply, State}
    end;
handle_cast({reduceend, From, TID}, State) when is_pid(From) ->
    io:format("Reduce End => ~w~n", [From]),
    case lists:keyfind(From, 1, State#state.tasks) of
        {From, Parent} ->
            Result = ets:foldl(fun(E, AS)-> [E | AS] end, [], TID),
            Parent ! {self(), Result},
            Tasks = lists:keydelete(From, 1, State#state.tasks),
            {noreply, State#state{tasks = Tasks}};
        _ ->
            {noreply, State}
    end;
handle_cast(_Request, State) ->
    {noreply, State}.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Handles all the non call/cast messages.
%% @spec
%% handle_info(Info :: timeout() | term(), State :: #state{}) ->
%%      {noreply, NewState :: #state{}} |
%%      {noreply, NewState :: #state{}, timeout() | hibernate} |
%%      {stop, Reason :: term(), NewState :: #state{}}
%% @end
%%-------------------------------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% This is called by a gen_server when it is about to terminate. It should be the
%% opposite of Module:init/1 and do any necessary cleaning up. When it returns, the
%% gen_server terminates with Reason. The return value is ignored.
%% @spec
%% terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
%%      State :: #state{}) -> term()
%% @end
%%-------------------------------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Converts the process state when the code is changed.
%% @spec
%% code_change(OldVsn :: term() | {down, term()}, State :: #state{},
%%      Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

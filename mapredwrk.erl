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
-module(mapredwrk).
-author("Gorka Suárez García").
-behaviour(gen_server).
-export([
    start_link/3, send_start_map/2, send_start_reduce/3,
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

%%===========================================================================================
%% Data
%%===========================================================================================

-record(state, {node, master, info}).

%%===========================================================================================
%% Interface
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Starts the server with a link.
%% @spec
%% start_link(Node::number(), Master::pid(), Info::[any()]) ->
%%      ignore | {ok, Pid :: pid()} | {error, Reason :: term()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec start_link(Node::number(), Master::pid(), Info::[any()]) ->
    ignore | {ok, Pid :: pid()} | {error, Reason :: term()}.
start_link(Node, Master, Info) when is_number(Node), Node > 0,
    is_pid(Master), is_list(Info) ->
    gen_server:start_link(?MODULE, {Node, Master, Info}, []).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Send the start map task.
%% @spec
%% send_start_map(PID::pid(), Fmap::fun((any()) -> any())) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec send_start_map(PID::pid(), Fmap::fun((any()) -> any())) -> any().
send_start_map(PID, Fmap) when is_function(Fmap) ->
    gen_server:cast(PID, {startmap, self(), Fmap}).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Send the start reduce task.
%% @spec
%% send_start_reduce(PID::pid(), Freduce::fun((any()) -> any()), MInfo::[any()]) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec send_start_reduce(PID::pid(), Freduce::fun((any()) -> any()), MInfo::[any()]) -> any().
send_start_reduce(PID, Freduce, MInfo) when is_function(Freduce), is_list(MInfo) ->
    gen_server:cast(PID, {startreduce, self(), Freduce, MInfo}).

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
init({Node, Master, Info}) when is_number(Node), Node > 0,
    is_pid(Master), is_list(Info) ->
    {ok, #state{node = Node, master = Master, info = Info}}.

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
handle_cast({startmap, From, Fmap}, State) when is_pid(From), is_function(Fmap) ->
    % We'll launch a process to launch all the map calls:
    spawn_link(
        fun() ->
            % Here we'll launch each map calling, sending the finished signal at the end:
            PID = self(), N = length(State#state.info),
            [spawn_link(fun() -> From ! Fmap(V), PID ! finished end) || V <- State#state.info],
            % We'll loop waiting to collect all the finished signals:
            Receiver =
                fun Loop(I) ->
                    case I < N of
                        true -> receive finished -> Loop(I + 1) end;
                        _ -> From ! {'end'}
                    end
                end,
            Receiver(0)
        end),
    {noreply, State};
handle_cast({startreduce, From, Freduce, []}, State) when is_pid(From),
    is_function(Freduce) ->
    % Nothing to reduce here, so we'll only send the end signal:
    From ! {'end'},
    {noreply, State};
handle_cast({startreduce, From, Freduce, MInfo}, State) when is_pid(From),
    is_function(Freduce), is_list(MInfo) ->
    % We'll launch a process to launch all the reduce calls:
    spawn_link(
        fun() ->
            % Here we'll launch each reduce calling, sending the finished signal at the end:
            PID = self(), N = length(MInfo),
            MakeReduce =
                fun(V) ->
                    case V of
                        {_, VS} when is_list(VS) -> From ! Freduce(V);
                        _ -> ok
                    end
                end,
            [spawn_link(fun() -> MakeReduce(V), PID ! finished end) || V <- MInfo],
            % We'll loop waiting to collect all the finished signals:
            Receiver =
                fun Loop(I) ->
                    case I < N of
                        true -> receive finished -> Loop(I + 1) end;
                        _ -> From ! {'end'}
                    end
                end,
            Receiver(0)
        end),
    {noreply, State};
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

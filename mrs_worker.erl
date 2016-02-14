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
-module(mrs_worker).
-author("Gorka Suárez García").
-include("mrs_api.hrl").
-include("mrs_job.hrl").
-behaviour(gen_server).
-export([
    start/1, start_link/1, nsend_task/2, nsend_task_result/2, nsend_task_failed/3,
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

%%===========================================================================================
%% Data
%%===========================================================================================

-define(SERVER, ?MODULE).
-record(state, {planner, tasks = []}).

%%===========================================================================================
%% Interface
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Starts the server.
%% <br/><b>PlannerNode:</b> The node of the planner server.
%% @spec
%% start(PlannerNode::atom()) ->
%%      {ok, Pid :: pid()} | ignore | {error, Reason :: term()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec start(PlannerNode::atom()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start(PlannerNode) ->
    gen_server:start({local, ?SERVER}, ?MODULE, [PlannerNode], []).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Starts the server with a link.
%% <br/><b>PlannerNode:</b> The node of the planner server.
%% @spec
%% start_link(PlannerNode::atom()) ->
%%      {ok, Pid :: pid()} | ignore | {error, Reason :: term()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec start_link(PlannerNode::atom()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(PlannerNode) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [PlannerNode], []).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sends a task to the worker.
%% <br/><b>Node:</b> The node of the server.
%% <br/><b>Task:</b> The task to send.
%% @spec
%% nsend_task(Node::any(),Task::#mrs_task{}) -> 'ok'
%% @end
%%-------------------------------------------------------------------------------------------
-spec nsend_task(Node::any(),Task::#mrs_task{}) -> 'ok'.
nsend_task({?SERVER, Node}, Task) when is_record(Task, mrs_task) ->
    gen_server:cast({?SERVER, Node}, {execute, Task});
nsend_task(Node, Task) when is_record(Task, mrs_task) ->
    gen_server:cast({?SERVER, Node}, {execute, Task}).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sends the task's result to the worker.
%% <br/><b>Node:</b> The node of the server.
%% <br/><b>Task:</b> The task to send.
%% @spec
%% nsend_task_result(Node::any(),Task::#mrs_task{}) -> 'ok'
%% @end
%%-------------------------------------------------------------------------------------------
-spec nsend_task_result(Node::any(),Task::#mrs_task{}) -> 'ok'.
nsend_task_result({?SERVER, Node}, Task) when is_record(Task, mrs_task) ->
    gen_server:cast({?SERVER, Node}, {result, Task});
nsend_task_result(Node, Task) when is_record(Task, mrs_task) ->
    gen_server:cast({?SERVER, Node}, {result, Task}).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sends a task failure notification.
%% <br/><b>Node:</b> The node of the server.
%% <br/><b>Task:</b> The task to send.
%% <br/><b>Reasson:</b> The reasson of the failure.
%% @spec
%% nsend_task_failed(Node::any(),Task::#mrs_task{},Reasson::any()) -> 'ok'
%% @end
%%-------------------------------------------------------------------------------------------
-spec nsend_task_failed(Node::any(),Task::#mrs_task{},Reasson::any()) -> 'ok'.
nsend_task_failed({?SERVER, Node}, Task, Reasson) when is_record(Task, mrs_task) ->
    gen_server:cast({?SERVER, Node}, {?ERROR, Reasson, Task});
nsend_task_failed(Node, Task, Reasson) when is_record(Task, mrs_task) ->
    gen_server:cast({?SERVER, Node}, {?ERROR, Reasson, Task}).

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
init([PlannerNode]) ->
    process_flag(trap_exit, true),
    {ok, #state{planner = PlannerNode}}.

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
handle_cast({execute, Task}, State) ->
    TS = [Task | State#state.tasks],
    mrs_jtask:launch_from_worker(node(), Task),
    {noreply, State#state{tasks = TS}};
handle_cast({result, Task}, State) ->
    IsTask = fun(T) -> T#mrs_task.ref =:= Task#mrs_task.ref end,
    TS = [T || T <- State#state.tasks, not IsTask(T)],
    case length(lists:filter(IsTask, State#state.tasks)) > 0 of
        true -> mrs_planner:nsend_task_result(State#state.planner, Task);
        _ -> ok
    end,
    {noreply, State#state{tasks = TS}};
handle_cast({?ERROR, Reasson, Task}, State) ->
    TS = [T || T <- State#state.tasks, T#mrs_task.qref =/= Task#mrs_task.qref],
    mrs_planner:nsend_task_failed(State#state.planner, Task, Reasson),
    {noreply, State#state{tasks = TS}};
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
handle_info({'EXIT', _PID, normal}, State) ->
    {noreply, State};
handle_info({'EXIT', _PID, shutdown}, State) ->
    {noreply, State};
handle_info({'EXIT', _PID, _Reasson}, State) ->
    TS = mrs_jtask:relaunch_from_worker(node(), State#state.tasks),
    {noreply, State#state{tasks = TS}};
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

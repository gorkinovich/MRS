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
-module(mrs_planner).
-author("Gorka Suárez García").
-include("mrs_api.hrl").
-include("mrs_job.hrl").
-behaviour(gen_server).
-export([
    start/0, start_link/0, send_query/1, send_query/2, nsend_query/2, receive_query/1,
    send_task_result/1, nsend_task_result/2, send_task_failed/2, nsend_task_failed/3,
    get_query_result/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

%%===========================================================================================
%% Data
%%===========================================================================================

-define(SERVER, ?MODULE).
-define(DB, planner_dets).
-define(DB_FILE, "planner_dets.bin").
-define(DB_STATE_KEY, server_state).
-define(HOSTS_FILE, "hosts.list").
-record(state, {workers, jobs}).

%%===========================================================================================
%% Interface
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @spec
%% start() -> ignore | {ok, Pid :: pid()} | {error, Reason :: term()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec start() ->
    ignore | {ok, Pid :: pid()} | {error, Reason :: term()}.
start() ->
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Starts the server with a link.
%% @spec
%% start_link() -> ignore | {ok, Pid :: pid()} | {error, Reason :: term()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec start_link() ->
    ignore | {ok, Pid :: pid()} | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sends a query to execute it.
%% <br/><b>Query:</b> The query to send.
%% @spec
%% send_query(Query::#mrs_query{}) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec send_query(Query::#mrs_query{}) -> any().
send_query(Query) when is_record(Query, mrs_query) ->
    gen_server:call(?SERVER, Query).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sends a query to execute it.
%% <br/><b>ServerRef:</b> The server handler.
%% <br/><b>Query:</b> The query to send.
%% @spec
%% send_query(ServerRef::any(),Query::#mrs_query{}) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec send_query(ServerRef::any(),Query::#mrs_query{}) -> any().
send_query(ServerRef, Query) when is_record(Query, mrs_query) ->
    gen_server:call(ServerRef, Query).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sends a query to execute it.
%% <br/><b>Node:</b> The node of the server.
%% <br/><b>Query:</b> The query to send.
%% @spec
%% nsend_query(Node::any(),Query::#mrs_query{}) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec nsend_query(Node::any(),Query::#mrs_query{}) -> any().
nsend_query({?SERVER, Node}, Query) when is_record(Query, mrs_query) ->
    gen_server:call({?SERVER, Node}, Query);
nsend_query(Node, Query) when is_record(Query, mrs_query) ->
    gen_server:call({?SERVER, Node}, Query).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Receives the result of a sended query.
%% <br/><b>Key:</b> The key of the job.
%% @spec
%% receive_query(Key::atom()) -> maybe_improper_list() | {'mrs:error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec receive_query(Key::atom()) -> maybe_improper_list() | {?ERROR,any()}.
receive_query(Key) when is_atom(Key) ->
    Owner = self(),
    receive
        {Key, Owner, {ok, Result}} when is_list(Result) -> Result;
        {Key, Owner, {?ERROR, Reasson}} -> {?ERROR, Reasson};
        _ -> {?ERROR, unknown}
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sends the task's result.
%% <br/><b>Task:</b> The task to send.
%% @spec
%% send_task_result(Task::#mrs_task{}) -> 'ok'
%% @end
%%-------------------------------------------------------------------------------------------
-spec send_task_result(Task::#mrs_task{}) -> 'ok'.
send_task_result(Task) when is_record(Task, mrs_task) ->
    gen_server:cast(?SERVER, {result, Task}).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sends the task's result.
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
%% <br/><b>Task:</b> The task to send.
%% <br/><b>Reasson:</b> The reasson of the failure.
%% @spec
%% send_task_failed(Task::#mrs_task{},Reasson::any()) -> 'ok'
%% @end
%%-------------------------------------------------------------------------------------------
-spec send_task_failed(Task::#mrs_task{},Reasson::any()) -> 'ok'.
send_task_failed(Task, Reasson) when is_record(Task, mrs_task) ->
    gen_server:cast(?SERVER, {?ERROR, Reasson, Task}).

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

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the result of a finished query.
%% <br/><b>QueryKey:</b> The key of the query.
%% @spec
%% get_query_result(QueryKey::reference() | atom()) -> [any()] | {'mrs:error', any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_query_result(QueryKey::reference() | atom()) -> any() | {?ERROR, any()}.
get_query_result(QueryKey) when is_reference(QueryKey) ->
    get_query_result(mrs_util:ref_to_atom(QueryKey));
get_query_result(QueryKey) when is_atom(QueryKey) ->
    gen_server:call(?SERVER, {get_result, QueryKey}).

%%===========================================================================================
%% Database
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Loads the state of the server from the database.
%% @spec
%% load_state() -> #state{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec load_state() -> #state{}.
load_state() ->
    % Open the database and find the saved state tuple:
    mrs_db:open(?DB, ?DB_FILE),
    State =
        case mrs_db:find_first(?DB, ?DB_STATE_KEY) of
            % If the database has a saved state, load all the jobs:
            {?DB_STATE_KEY, Wpids, Jkeys} ->
                Jobs = mrs_db:find_heads(?DB, Jkeys),
                #state{workers = Wpids, jobs = Jobs};
            % If no data is found, make a new state:
            _ ->
                #state{workers = [], jobs = []}
        end,
    % Close the database and return the state:
    mrs_db:close(?DB),
    State.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Saves the state of the server in the database.
%% <br/><b>State:</b> The state data.
%% @spec
%% save_state(State::#state{}) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec save_state(State::#state{}) -> any().
save_state(State) ->
    % Open the database to save all the jobs:
    mrs_db:open(?DB, ?DB_FILE),
    mrs_db:update_all(?DB, State#state.jobs),
    % And then save the jobs keys and the current workers:
    Jkeys = [mrs_job:key(V) || V <- State#state.jobs],
    mrs_db:update(?DB, {?DB_STATE_KEY, State#state.workers, Jkeys}),
    mrs_db:close(?DB).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Saves a job in the database.
%% <br/><b>Job:</b> The job data.
%% @spec
%% save_job(Job::tuple()) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec save_job(Job::tuple()) -> any().
save_job(Job) ->
    mrs_db:open(?DB, ?DB_FILE),
    mrs_db:update(?DB, Job),
    mrs_db:close(?DB).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Updates a job in the database.
%% <br/><b>Job:</b> The job data.
%% <br/><b>Task:</b> The received task.
%% @spec
%% update_job(Jobs::[tuple()],Task::#mrs_task{}) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec update_job(Jobs::[tuple()],Task::#mrs_task{}) -> any().
update_job(Jobs, Task) when is_record(Task, mrs_task) ->
    case lists:keyfind(Task#mrs_task.qref, 1, Jobs) of
        Job when is_tuple(Job) -> save_job(Job);
        _ -> ok
    end.

%%===========================================================================================
%% Callbacks
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @spec
%% init(Args :: term()) -> ignore | {ok, State :: #state{}} |
%%      {ok, State :: #state{}, timeout() | hibernate} | {stop, Reason :: term()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec init(Args :: term()) -> ignore | {ok, State :: #state{}} |
    {ok, State :: #state{}, timeout() | hibernate} | {stop, Reason :: term()}.
init(_) ->
    try
        % Capture all the 'EXIT' signals:
        process_flag(trap_exit, true),
        % Read the list of hosts:
        Hosts = mrs_util:read_lines(?HOSTS_FILE, []),
        % Get all the worker nodes and launch a worker server:
        CurrentNode = node(),
        WorkerNodes = mrs_util:find_other_nodes_from_hosts(Hosts),
        [spawn(WN, fun() -> mrs_worker:start(CurrentNode) end) || WN <- WorkerNodes],
        Workers = [{mrs_worker, WN} || WN <- WorkerNodes],
        % Load the state from the database:
        State = load_state(),
        % Join the workers data and finish the initialization:
        NextWorkers = lists:usort(Workers ++ State#state.workers),
        % TODO: Relaunch saved jobs in the database...
        {ok, State#state{workers = NextWorkers}}
    catch
        Error:Exception  ->
            {stop, {Error, Exception}}
    end.

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
handle_call(Query, {From, _}, State) when is_record(Query, mrs_query) ->
    try
        %io:format("Query Received From: ~p~n~p~n~n", [From, Query]),
        Job = mrs_job:new(From, Query),
        save_job(Job),
        case mrs_jlogic:start(Job, State#state.workers) of
            ?JOB_FINISHED ->
                {reply, ?JOB_FINISHED, State};
            StartedJob ->
                JS2 = [StartedJob | State#state.jobs],
                {reply, {ok, mrs_job:key(StartedJob)}, State#state{jobs = JS2}}
        end
    catch
        Error:Exception ->
            {reply, {?ERROR, {Error,Exception}}, State}
    end;
handle_call({get_result, QueryKey}, _From, State) when is_atom(QueryKey) ->
    try
        %io:format("Get Result With Query Key: ~p~n~n", [QueryKey]),
        case lists:keyfind(QueryKey, 1, State#state.jobs) of
            Job when is_tuple(Job) ->
                case mrs_jlogic:is_finished(Job) of
                    true -> mrs_job:data(Job);
                    _ -> {?ERROR, not_finished}
                end;
            _ ->
                {?ERROR, not_found}
        end
    catch
        Error:Exception ->
            {reply, {?ERROR, {Error,Exception}}, State}
    end;
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
handle_cast({result, Task}, State) when is_record(Task, mrs_task) ->
    Jobs = State#state.jobs,
    WorkerNodes = State#state.workers,
    JS2 = mrs_jlogic:task_received(Jobs, WorkerNodes, Task),
    % TODO: Drive database too slow...
    %update_job(JS2, Task),
    {noreply, State#state{jobs = JS2}};
handle_cast({?ERROR, Reasson, Task}, State) when is_record(Task, mrs_task) ->
    %io:format("Task ERROR(~p):~n~p~n~n", [Reasson, Task]),
    Jobs = State#state.jobs,
    JS2 = mrs_jlogic:task_failed(Jobs, Task, Reasson),
    update_job(JS2, Task),
    {noreply, State#state{jobs = JS2}};
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
handle_info({'EXIT', PID, _Reasson}, State) ->
    JS = mrs_jlogic:relaunch_task(State#state.jobs, PID),
    {noreply, State#state{jobs = JS}};
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
terminate(_Reason, State) ->
    save_state(State).

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

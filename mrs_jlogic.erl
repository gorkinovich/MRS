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
-module(mrs_jlogic).
-author("Gorka Suárez García").
-include("mrs_api.hrl").
-include("mrs_job.hrl").
-export([
    start/2, task_received/3, task_failed/3, relaunch_task/2, is_finished/1,
    assign_any_pending_chunk/2, unassign_pending_chunk/2
]).

%%===========================================================================================
%% Logic operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Executes the new job received processing algorithm.
%% <br/><b>Job:</b> The job to change.
%% <br/><b>WorkerNodes:</b> The list with the worker's nodes.
%% <br/>(<b>Note:</b> Returns a new job with the changes if it's not finished.)
%% @spec
%% start(Job::tuple(),WorkerNodes::[tuple()]) -> 'finished' | tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec start(Job::tuple(),WorkerNodes::[tuple()]) -> 'finished' | tuple().
start(Job, WorkerNodes) ->
    case is_finished(Job) of
        true -> ?JOB_FINISHED;
        _ -> start_make_chunks(Job, WorkerNodes)
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Makes chunks from the current data and send them to the worker's nodes.
%% <br/><b>Job:</b> The job to change.
%% <br/><b>WorkerNodes:</b> The list with the worker's nodes.
%% <br/>(<b>Note:</b> Returns a new job with the changes.)
%% @spec
%% start_make_chunks(Job::tuple(),WorkerNodes::[any()]) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec start_make_chunks(Job::tuple(),WorkerNodes::[any()]) -> tuple().
start_make_chunks(Job, WorkerNodes) ->
    case is_parallelizable(mrs_job:current_step(Job)) of
        % If the step is parallelizable, we'll make the chunks and start
        % the sending process:
        true ->
            J2 = data_to_pending_chunks(Job, length(WorkerNodes)),
            start_sending_chunks(J2, WorkerNodes);
        % If the step isn't parallelizable, we'll make only one task and
        % launch the execution in the current node:
        _ ->
            Task = make_task(Job),
            PID = mrs_jtask:launch_from_planner(node(), Task),
            mrs_job:pending_chunks(Job, [PID])
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Sends any unassigned pending chunk to a list of worker's nodes.
%% <br/><b>Job:</b> The job to change.
%% <br/><b>WorkerNodes:</b> The list with the worker's nodes.
%% <br/>(<b>Note:</b> Returns a new job with the changes.)
%% @spec
%% start_sending_chunks(Job::tuple(),WorkerNodes::[any()]) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec start_sending_chunks(Job::tuple(),WorkerNodes::[any()]) -> tuple().
start_sending_chunks(Job, []) ->
    Job;
start_sending_chunks(Job, [WN|WNS]) ->
    case any_chunk_to_send(Job) of
        % If there is any pending chunk to send, we'll try to get one of them:
        true ->
            TaskRef = make_ref(),
            case get_any_pending_chunk(Job, TaskRef) of
                % If we get a chunk, make the task, send it to the current worker
                % node, update the sended chunks count and go to the next iteration:
                {J2, {Index, Data}} ->
                    Step = mrs_job:current_step(Job),
                    Task = make_task(TaskRef, WN, mrs_job:key(Job), Step, Index, Data),
                    mrs_worker:nsend_task(WN, Task),
                    J3 = mrs_job:sended_chunks(J2, mrs_job:sended_chunks(J2) + 1),
                    start_sending_chunks(J3, WNS);
                % If we get no chunk, change nothing:
                {J2, nothing} ->
                    J2
            end;
        _->
            Job
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Makes a new task.
%% <br/><b>Job:</b> The job with the task's data.
%% <br/>(<b>Note:</b> This will be called to make a task to be done in the planners node.)
%% @spec
%% make_task(Job::tuple()) -> #mrs_task{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec make_task(Job::tuple()) -> #mrs_task{}.
make_task(Job) ->
    #mrs_task{worker = node(), qref = mrs_job:key(Job), step = mrs_job:current_step(Job),
        index = 0, chunk = mrs_job:data(Job)}.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Makes a new task.
%% <br/><b>TaskRef:</b> The reference of the task.
%% <br/><b>Node:</b> The worker's node.
%% <br/><b>Key:</b> The key of the job.
%% <br/><b>Step:</b> The current step of the query.
%% <br/><b>Index:</b> The index of the chunk.
%% <br/><b>Data:</b> The data of the chunk.
%% <br/>(<b>Note:</b> This will be called to make a task to be sended to a worker.)
%% @spec
%% make_task(TaskRef::reference(),Node::atom(),Key::atom(),
%%      Step::#mrs_step{},Index::number(),Data::[any]) -> #mrs_task{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec make_task(TaskRef::reference(),Node::atom(),Key::atom(),
    Step::#mrs_step{},Index::number(),Data::[any]) -> #mrs_task{}.
make_task(TaskRef, Node, Key, Step, Index, Data) ->
    #mrs_task{ref = TaskRef, worker = Node, qref = Key, step = Step,
        index = Index, chunk = Data}.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Executes the task received processing algorithm.
%% <br/><b>Job:</b> The job to change.
%% <br/><b>WorkerNodes:</b> The list with the worker's nodes.
%% <br/><b>Task:</b> The received task.
%% <br/>(<b>Note:</b> Returns a new list of jobs with the changes.)
%% @spec
%% task_received(Jobs::[tuple()],WorkerNodes::[atom()],Task::#mrs_task{}) -> [tuple()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec task_received(Jobs::[tuple()],WorkerNodes::[atom()],Task::#mrs_task{}) -> [tuple()].
task_received(Jobs, WorkerNodes, Task) when is_record(Task, mrs_task) ->
    case lists:keyfind(Task#mrs_task.qref, 1, Jobs) of
        % If there is a job with the same key, we'll check if it's finished:
        Job when is_tuple(Job) ->
            case is_finished(Job) of
                % If it's finished, we'll discard the changes:
                true ->
                    Jobs;
                % If it's not finished, we'll check if it's parallelizable:
                _ ->
                    JR =
                        case is_parallelizable(Task#mrs_task.step) of
                            % When it's parallelizable, we'll add the received chunk,
                            % and send the next chunk to the current worker node:
                            true ->
                                J2 = add_received_chunk(Job, Task#mrs_task.ref,
                                    Task#mrs_task.index, Task#mrs_task.chunk),
                                send_next_chunk(J2, Task#mrs_task.worker, WorkerNodes);
                            % When it's not parallelizable, we'll set the current
                            % data, and go to the next step in the job:
                            _ ->
                                J2 = mrs_job:data(Job, Task#mrs_task.chunk),
                                job_next_step(J2, WorkerNodes)
                        end,
                    lists:keyreplace(Task#mrs_task.qref, 1, Jobs, JR)
            end;
        _ ->
            Jobs
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Executes the task failed processing algorithm.
%% <br/><b>Job:</b> The job to change.
%% <br/><b>Task:</b> The received task.
%% <br/><b>Reasson:</b> The fail reasson.
%% <br/>(<b>Note:</b> Returns a new list of jobs with the changes.)
%% @spec
%% task_failed(Jobs::[tuple()],Task::#mrs_task{},Reasson::any()) -> [tuple()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec task_failed(Jobs::[tuple()],Task::#mrs_task{},Reasson::any()) -> [tuple()].
task_failed(Jobs, Task, Reasson) when is_record(Task, mrs_task) ->
    case lists:keyfind(Task#mrs_task.qref, 1, Jobs) of
        % If there is a job with the same key, we'll check if it's finished:
        Job when is_tuple(Job) ->
            case is_finished(Job) of
                % If it's finished, we'll discard the changes:
                true ->
                    Jobs;
                % If it's not finished, we'll change its status:
                _ ->
                    JR = mrs_job:current_step(Job, {?JOB_FINISHED, Reasson}),
                    lists:keyreplace(Task#mrs_task.qref, 1, Jobs, JR)
            end;
        _ ->
            Jobs
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Sends the next chunk in the current step of a job. But if the step has been finished,
%% it'll move the job to the next step in the query.
%% <br/><b>Job:</b> The job to change.
%% <br/><b>WorkerNode:</b> The worker's node.
%% <br/><b>WorkerNodes:</b> The list with the worker's nodes.
%% <br/>(<b>Note:</b> Returns a new job with the changes.)
%% @spec
%% send_next_chunk(Job::tuple(),WorkerNode::atom(),WorkerNodes::[atom()]) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec send_next_chunk(Job::tuple(),WorkerNode::atom(),WorkerNodes::[atom()]) -> tuple().
send_next_chunk(Job, WorkerNode, WorkerNodes) ->
    case any_chunk_to_send(Job) of
        % If there is any pending chunk to send, we'll send another one to
        % the current worker node:
        true ->
            start_sending_chunks(Job, [WorkerNode]);
        % If we haven't any pending chunk, we'll check if all of them has
        % been received or not:
        _ ->
            case all_chunks_received(Job) of
                % If all has been received, we'll join the result and go
                % to the next step of the job:
                true ->
                    J2 = join_received_chunks(Job),
                    job_next_step(J2, WorkerNodes);
                % If not, we'll resend any pending chunk:
                _ ->
                    resend_any_pending_chunk(Job, WorkerNode)
            end
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Joins the received chunks into a final data.
%% <br/><b>Job:</b> The job to change.
%% <br/>(<b>Note:</b> Returns a new job with the changes.)
%% @spec
%% join_received_chunks(Job::tuple()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec join_received_chunks(Job::tuple()) -> tuple().
join_received_chunks(Job) ->
    Step = mrs_job:current_step(Job),
    Chunks = mrs_job:received_chunks(Job),
    Data = join_received_chunks(Step, Chunks),
    mrs_job:data(Job, Data).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Joins a collection of chunks into a list or value.
%% <br/><b>Step:</b> The query's step.
%% <br/><b>Chunks:</b> The list of chunks.
%% @spec
%% join_received_chunks(Step::#mrs_step{},Chunks::[tuple()]) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec join_received_chunks(Step::#mrs_step{},Chunks::[tuple()]) -> any().
join_received_chunks(Step, Chunks) ->
    C2 = join_chunks(Chunks),
    case Step#mrs_step.mode of
        Mode when (Mode =:= ?GROUPBY_MODE) orelse (Mode =:= ?GROUPBYKEY_MODE) ->
            mrs_util:join_pairs(C2);
        Mode when (Mode =:= ?REDUCE_MODE) ->
            mrs_util:aggregate(Step#mrs_step.params, C2);
        _ ->
            C2
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Joins a collection of chunks into a list.
%% <br/><b>Chunks:</b> The list of chunks.
%% @spec
%% join_chunks(Chunks::[tuple()]) -> [any()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec join_chunks(Chunks::[tuple()]) -> [any()].
join_chunks(Chunks) ->
    SC = lists:keysort(1, Chunks),
    lists:append([L || {_,L} <- SC]).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Resends any pending chunk in a job to a worker node.
%% <br/><b>Job:</b> The job to change.
%% <br/><b>WorkerNode:</b> The worker's node.
%% <br/>(<b>Note:</b> Returns a new job with the changes.)
%% @spec
%% resend_any_pending_chunk(Job::tuple(),WorkerNode::atom()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec resend_any_pending_chunk(Job::tuple(),WorkerNode::atom()) -> tuple().
resend_any_pending_chunk(Job, _WorkerNode) ->
    % First, check if there is any pending chunk yet:
    PChunks = mrs_job:pending_chunks(Job),
    case length(PChunks) > 0 of
        true ->
            % When pending chunks, get the first in the list, make a new task
            % and send it to the worker:
            % TODO: Resend pending chunks should be done only if a node falls...
            %{Index, TaskRef, Data} = hd(PChunks),
            %Task = make_task(TaskRef, WorkerNode, mrs_job:key(Job),
            %    mrs_job:current_step(Job), Index, Data),
            %mrs_worker:nsend_task(WorkerNode, Task),
            Job;
        _ ->
            % If no pending chunks, no changes are made:
            Job
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Sets the next step of the query in a job.
%% <br/><b>Job:</b> The job to change.
%% <br/><b>WorkerNodes:</b> The list with the worker's nodes.
%% <br/>(<b>Note:</b> Returns a new job with the changes.)
%% @spec
%% job_next_step(Job::tuple(),WorkerNodes::[atom()]) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec job_next_step(Job::tuple(),WorkerNodes::[atom()]) -> tuple().
job_next_step(Job, WorkerNodes) ->
    % First, we'll get the next step in the query and then check if it's finished:
    J2 = query_next_step(Job),
    case is_finished(J2) of
        % When it's finished, we'll return the result of the job:
        true -> return_finished_query(J2);
        % If it's not finished, we'll make new chunks and send them to the workers:
        _ -> start_make_chunks(J2, WorkerNodes)
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Sets the next step of the query in a job.
%% <br/><b>Job:</b> The job to change.
%% <br/>(<b>Note:</b> Returns a new job with the changes.)
%% @spec
%% query_next_step(Job::tuple()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec query_next_step(Job::tuple()) -> tuple().
query_next_step(Job) ->
    % First, we'll clean the chunk data and get the next step in the queue:
    J2 = clean_chunk_data(Job),
    case queue:out(mrs_job:next_steps(J2)) of
        % If the queue is not empty, we'll change the current step and
        % update the next steps queue:
        {{value, CurrentStep}, NextSteps} ->
            J3 = mrs_job:current_step(J2, CurrentStep),
            mrs_job:next_steps(J3, NextSteps);
        % If the queue is empty, we'll set the job as finished:
        {empty, _} ->
            mrs_job:current_step(J2, ?JOB_FINISHED)
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Resets the data used to handle the chunks in a job.
%% <br/><b>Job:</b> The job to change.
%% <br/>(<b>Note:</b> Returns a new job with the changes.)
%% @spec
%% clean_chunk_data(Job::tuple()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec clean_chunk_data(Job::tuple()) -> tuple().
clean_chunk_data(Job) ->
    J2 = mrs_job:total_chunks(Job, 0),
    J3 = mrs_job:sended_chunks(J2, 0),
    J4 = mrs_job:pending_chunks(J3, []),
    mrs_job:received_chunks(J4, []).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Sends to the owner of the query the job's result if it's finished.
%% <br/><b>Job:</b> The job with the query.
%% <br/>(<b>Note:</b> Returns the job without changes.)
%% @spec
%% return_finished_query(Job::tuple()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec return_finished_query(Job::tuple()) -> tuple().
return_finished_query(Job) ->
    Key = mrs_job:key(Job),
    Owner = mrs_job:owner(Job),
    case mrs_job:current_step(Job) of
        ?JOB_FINISHED -> Owner ! {Key, Owner, {ok, mrs_job:data(Job)}};
        {?JOB_FINISHED, ok} -> Owner ! {Key, Owner, {ok, mrs_job:data(Job)}};
        {?JOB_FINISHED, {?ERROR, Reasson}} -> Owner ! {Key, Owner, {?ERROR, Reasson}};
        _ -> ok
    end,
    Job.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Executes the task exit signal processing algorithm.
%% <br/><b>Jobs:</b> The list of jobs of the planner.
%% <br/><b>PID:</b> The process ID of the killed task.
%% <br/>(<b>Note:</b> Returns a new list of jobs with the changes.)
%% @spec
%% relaunch_task(Jobs::[tuple()], PID::pid()) -> [tuple()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec relaunch_task(Jobs::[tuple()], PID::pid()) -> [tuple()].
relaunch_task(Jobs, PID) ->
    JL = [J || J <- Jobs, mrs_job:pending_chunks(J) == [PID]],
    case length(JL) > 0 of
        true ->
            Job = hd(JL),
            Task = make_task(Job),
            NPID = mrs_jtask:launch_from_planner(node(), Task),
            JR = mrs_job:pending_chunks(Job, [NPID]),
            lists:keyreplace(mrs_job:key(JR), 1, Jobs, JR);
        _ ->
            Jobs
    end.

%%===========================================================================================
%% Check operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Checks if a job is finished or not.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% is_finished(T::tuple()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec is_finished(T::tuple()) -> boolean().
is_finished(T) ->
    case mrs_job:current_step(T) of
        ?JOB_FINISHED      -> true;
        {?JOB_FINISHED, _} -> true;
        _                  -> false
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Checks if the job contains any data to make new pending chunks.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% any_data_to_make_chunks(T::tuple()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec any_data_to_make_chunks(T::tuple()) -> boolean().
any_data_to_make_chunks(T) ->
    (length(mrs_job:data(T)) > 0) andalso (mrs_job:chunk_size(T) > 0).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Checks if the job contains any pending chunks to send.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% any_chunk_to_send(T::tuple()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec any_chunk_to_send(T::tuple()) -> boolean().
any_chunk_to_send(T) ->
    mrs_job:total_chunks(T) > mrs_job:sended_chunks(T).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Checks if the job has received all the sended chunks.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% all_chunks_received(T::tuple()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec all_chunks_received(T::tuple()) -> boolean().
all_chunks_received(T) ->
    length(mrs_job:received_chunks(T)) =:= mrs_job:total_chunks(T).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Checks if a step mode is parallelizable or not.
%% <br/><b>Mode:</b> The mode of the step.
%% @spec
%% is_parallelizable(Mode::atom() | #mrs_step{}) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec is_parallelizable(Mode::atom() | #mrs_step{}) -> boolean().
is_parallelizable(Step) when is_record(Step, mrs_step) ->
    is_parallelizable(Step#mrs_step.mode);
is_parallelizable(Mode) when is_atom(Mode) ->
    (Mode =:= ?FILTER_MODE)     orelse (Mode =:= ?FLATMAP_MODE)            orelse
    (Mode =:= ?MAP_MODE)        orelse (Mode =:= ?REDUCEBYKEY_MODE)        orelse
    (Mode =:= ?ZIPWUID_MODE)    orelse (Mode =:= ?AGGREGATEBYKEY_MODE)     orelse
    (Mode =:= ?COUNTBYKEY_MODE) orelse (Mode =:= ?COUNTDISTINCTBYKEY_MODE) orelse
    (Mode =:= ?FOLDRBYKEY_MODE) orelse (Mode =:= ?FOLDLBYKEY_MODE)         orelse
    (Mode =:= ?GROUPBY_MODE)    orelse (Mode =:= ?GROUPBYKEY_MODE)         orelse
    (Mode =:= ?REDUCE_MODE)     orelse (Mode =:= ?SORTBYKEY_MODE).

%%===========================================================================================
%% Chunks
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Moves the current data to a pending chunks collection.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>Workers:</b> The number of workers of the system.
%% @spec
%% data_to_pending_chunks(T::tuple(),Workers::number()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec data_to_pending_chunks(T::tuple(),Workers::number()) -> tuple().
data_to_pending_chunks(T, Workers) ->
    make_pending_chunks(calc_chunk_size(T, Workers)).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Sets the chunk size in a job by a given number of workers.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>Workers:</b> The number of workers of the system.
%% @spec
%% calc_chunk_size(T::tuple(),Workers::number()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec calc_chunk_size(T::tuple(),Workers::number()) -> tuple().
calc_chunk_size(T, Workers) when is_number(Workers) ->
    Len = length(mrs_job:data(T)),
    Factor =
        case Len < (Workers * ?MAX_CHUNK_ELEMS) of
            true -> Workers;
            _ -> ?MAX_CHUNK_ELEMS
        end,
    mrs_job:chunk_size(T, (Len div Factor) + 1).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Makes all the pending chunks of the current step with the available data.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% make_pending_chunks(T::tuple()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec make_pending_chunks(T::tuple()) -> tuple().
make_pending_chunks(T) ->
    case any_data_to_make_chunks(T) of
        true -> make_pending_chunks(make_new_pending_chunk(T));
        _ -> T
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Makes a pending chunks of the current step with the available data.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% make_new_pending_chunk(T::tuple()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec make_new_pending_chunk(T::tuple()) -> tuple().
make_new_pending_chunk(T) ->
    Data = mrs_job:data(T),
    Size = mrs_job:chunk_size(T),
    DataSize = length(Data),
    if
        % We have more data than a chunk size:
        Size > 0, DataSize > Size ->
            {CData, NextData} = lists:split(Size, Data),
            mrs_job:data(add_new_pending_chunk(T, CData), NextData);
        % We have less data than a chunk size:
        Size > 0, DataSize > 0 ->
            mrs_job:data(add_new_pending_chunk(T, Data), []);
        % We have a problem right here:
        true ->
            T
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Adds a pending chunks to the current step with a given data.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>Data:</b> The data used to make the chunk.
%% @spec
%% add_new_pending_chunk(T::tuple(),Data::[any()]) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec add_new_pending_chunk(T::tuple(),Data::[any()]) -> tuple().
add_new_pending_chunk(T, Data) ->
    Total = mrs_job:total_chunks(T),
    Chunks = mrs_job:pending_chunks(T),
    Chunk = {Total + 1, ?PCHUNK_NOWORKER, Data},
    T2 = mrs_job:total_chunks(T, Total + 1),
    mrs_job:pending_chunks(T2, [Chunk | Chunks]).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Gets and assigns a reference to any pending chunk without an assigned task.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>TaskRef:</b> The reference of the task.
%% @spec
%% get_any_pending_chunk(T::tuple(),TaskRef::reference()) ->
%%      {tuple(), {number(), [any()]} | 'nothing'}
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_any_pending_chunk(T::tuple(),TaskRef::reference()) ->
    {tuple(), {number(), [any()]} | 'nothing'}.
get_any_pending_chunk(T, TaskRef) ->
    PChunks = mrs_job:pending_chunks(T),
    case lists:keyfind(?PCHUNK_NOWORKER, 2, PChunks) of
        % If there is an unassigned pending chunk, we'll replace it with
        % a new tuple with the assigned worker:
        {Index, _, CData} when is_number(Index), is_list(CData) ->
            PC2 = lists:keyreplace(Index, 1, PChunks, {Index, TaskRef, CData}),
            {mrs_job:pending_chunks(T, PC2), {Index, CData}};
        % If everything is assigned, change nothing:
        _ ->
            {T, nothing}
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Assigns a reference to any pending chunk without an assigned task.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>TaskRef:</b> The reference of the task.
%% @spec
%% assign_any_pending_chunk(T::tuple(),TaskRef::reference()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec assign_any_pending_chunk(T::tuple(),TaskRef::reference()) -> tuple().
assign_any_pending_chunk(T, TaskRef) ->
    find_and_set_pending_chunk(T, ?PCHUNK_NOWORKER, 2, TaskRef).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Unassigns a process to a pending chunk with an assigned worker.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>Index:</b> The index of the chunk.
%% @spec
%% unassign_pending_chunk(T::tuple(),Index::number()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec unassign_pending_chunk(T::tuple(),Index::number()) -> tuple().
unassign_pending_chunk(T, Index) ->
    find_and_set_pending_chunk(T, Index, 1, ?PCHUNK_NOWORKER).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Finds a pending chunk with a given key and sets its task's reference.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>Key:</b> The key to find the chunk tuple.
%% <br/><b>Pos:</b> The position of the key in the tuple.
%% <br/><b>Ref:</b> The task's reference.
%% @spec
%% find_and_set_pending_chunk(T::tuple(), Key::any(), Pos::number(),
%%      Ref::reference()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_and_set_pending_chunk(T::tuple(), Key::any(), Pos::number(),
    Ref::reference()) -> tuple().
find_and_set_pending_chunk(T, Key, Pos, Ref) ->
    PChunks = mrs_job:pending_chunks(T),
    case lists:keyfind(Key, Pos, PChunks) of
        % If the index is found, we'll replace the task's reference:
        {Index, _, CData} when is_number(Index), is_list(CData) ->
            Chunk = {Index, Ref, CData},
            PC2 = lists:keyreplace(Index, 1, PChunks, Chunk),
            mrs_job:pending_chunks(T, PC2);
        % If the index is not found, change nothing:
        _ ->
            T
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Adds a received chunk, removing the previous pending sended chunk.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>Index:</b> The index of the chunk.
%% <br/><b>Data:</b> The received chunk of data.
%% @spec
%% add_received_chunk(T::tuple(), Ref::reference(), Index::number(),
%%      Data::[any()]) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec add_received_chunk(T::tuple(), Ref::reference(), Index::number(),
    Data::[any()]) -> tuple().
add_received_chunk(T, Ref, Index, Data) ->
    PChunks = mrs_job:pending_chunks(T),
    case lists:keyfind(Index, 1, PChunks) of
        % If the index is found, we'll remove it from the pending list and add
        % it to the received list if not exists:
        {Index, Ref, _} when is_number(Index) ->
            SChunks = mrs_job:received_chunks(T),
            PC2 = lists:keydelete(Index, 1, PChunks),
            T2 = mrs_job:pending_chunks(T, PC2),
            case lists:keyfind(Index, 1, SChunks) of
                {Index, _} -> T2;
                _ -> mrs_job:received_chunks(T2, [{Index, Data} | SChunks])
            end;
        % If the index is not found, change nothing:
        _ ->
            T
    end.

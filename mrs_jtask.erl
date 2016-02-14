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
-module(mrs_jtask).
-author("Gorka Suárez García").
-include("mrs_api.hrl").
-include("mrs_job.hrl").
-export([launch_from_planner/2, launch_from_worker/2, relaunch_from_worker/2]).

%%===========================================================================================
%% Launch operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Launches a task invoked by the planner server.
%% <br/><b>Node:</b> The node of the server.
%% <br/><b>Task:</b> The task to execute.
%% @spec
%% launch_from_planner(Node::atom(), Task::#mrs_task{}) -> pid()
%% @end
%%-------------------------------------------------------------------------------------------
-spec launch_from_planner(Node::atom(), Task::#mrs_task{}) -> pid().
launch_from_planner(Node, Task) when is_atom(Node), is_record(Task, mrs_task) ->
    OnError = fun(R) -> mrs_planner:nsend_task_failed(Node, Task, R) end,
    OnTask = fun(TR) -> mrs_planner:nsend_task_result(Node, TR) end,
    launch_function(Task#mrs_task.qref, fun() -> generic_execute(Task, OnError, OnTask) end).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Launches a task invoked by the worker server.
%% <br/><b>Node:</b> The node of the server.
%% <br/><b>Task:</b> The task to execute.
%% @spec
%% launch_from_worker(Node::atom(), Task::#mrs_task{}) -> pid()
%% @end
%%-------------------------------------------------------------------------------------------
-spec launch_from_worker(Node::atom(), Task::#mrs_task{}) -> pid().
launch_from_worker(Node, Task) when is_atom(Node), is_record(Task, mrs_task) ->
    OnError = fun(R) -> mrs_worker:nsend_task_failed(Node, Task, R) end,
    OnTask = fun(TR) -> mrs_worker:nsend_task_result(Node, TR) end,
    launch_task(Task, fun() -> generic_execute(Task, OnError, OnTask) end).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Executes a task and send the result.
%% <br/><b>Task:</b> The task to execute.
%% <br/><b>OnError:</b> The on error handler.
%% <br/><b>OnTask:</b> The on task result handler.
%% @spec
%% generic_execute(Task::#mrs_task{}, OnError::fun((any()) -> any()),
%%      OnTask::fun((any()) -> any())) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec generic_execute(Task::#mrs_task{}, OnError::fun((any()) -> any()),
    OnTask::fun((any()) -> any())) -> any().
generic_execute(Task, OnError, OnTask) ->
    try
        case execute(Task) of
            TR when is_record(TR, mrs_task) -> OnTask(TR);
            {?ERROR, Reasson} -> OnError(Reasson);
            _ -> OnError(unknown)
        end
    catch
        Error:Exception -> OnError({Error, Exception})
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Launches a task.
%% <br/><b>Task:</b> The task to execute.
%% <br/><b>Fun:</b> The function of the spawn.
%% @spec
%% launch_task(Task::#mrs_task{}, Fun::fun(() -> any())) -> pid()
%% @end
%%-------------------------------------------------------------------------------------------
-spec launch_task(Task::#mrs_task{}, Fun::fun(() -> any())) -> pid().
launch_task(Task, Fun) ->
    launch_function(get_pname(Task), Fun).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Launches a function.
%% <br/><b>Name:</b> The name of the process.
%% <br/><b>Fun:</b> The function of the spawn.
%% @spec
%% launch_function(Name::atom(), Fun::fun(() -> any())) -> pid()
%% @end
%%-------------------------------------------------------------------------------------------
-spec launch_function(Name::atom(), Fun::fun(() -> any())) -> pid().
launch_function(Name, Fun) ->
    PID = spawn_link(Fun),
    register(Name, PID),
    PID.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Relaunches all the current tasks without an execute function.
%% <br/><b>Node:</b> The node of the server.
%% <br/><b>Tasks:</b> The current tasks of the server.
%% @spec
%% relaunch_from_worker(Node::atom(), Tasks::[#mrs_task{}]) -> [#mrs_task{}]
%% @end
%%-------------------------------------------------------------------------------------------
-spec relaunch_from_worker(Node::atom(), Tasks::[#mrs_task{}]) -> [#mrs_task{}].
relaunch_from_worker(_Node, []) ->
    [];
relaunch_from_worker(Node, [T|TS]) ->
    case whereis(get_pname(T)) of
        undefined ->
            launch_from_worker(Node, T),
            [T|relaunch_from_worker(Node, TS)];
        _ ->
            [T|relaunch_from_worker(Node, TS)]
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Gets the name to register an execute process.
%% <br/><b>Task:</b> The task to execute.
%% @spec
%% get_pname(Task::#mrs_task{}) -> atom()
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_pname(Task::#mrs_task{}) -> atom().
get_pname(Task) ->
    mrs_util:ref_to_atom(Task#mrs_task.ref).

%%===========================================================================================
%% Execute operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Makes an error value.
%% <br/><b>Reasson:</b> The reasson of the error.
%% @spec
%% make_error(Reasson::any()) -> {'mrs:error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec make_error(Reasson::any()) -> {?ERROR,any()}.
make_error(Reasson) ->
    {?ERROR, Reasson}.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Gets an ascending comparer to sort data.
%% <br/><b>Selector:</b> The value selector to compare.
%% @spec
%% get_asc_cmp(Selector::fun((any()) -> any())) -> fun((any(),any()) -> boolean())
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_asc_cmp(Selector::fun((any()) -> any())) -> fun((any(),any()) -> boolean()).
get_asc_cmp(Selector) ->
    fun(A, B) -> Selector(A) =< Selector(B) end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Gets an descending comparer to sort data.
%% <br/><b>Selector:</b> The value selector to compare.
%% @spec
%% get_des_cmp(Selector::fun((any()) -> any())) -> fun((any(),any()) -> boolean())
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_des_cmp(Selector::fun((any()) -> any())) -> fun((any(),any()) -> boolean()).
get_des_cmp(Selector) ->
    fun(A, B) -> Selector(A) > Selector(B) end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Counts disctinct values inside a list.
%% <br/><b>Data:</b> The list with the values.
%% @spec
%% count_distinct(Data::[any()]) -> non_neg_integer()
%% @end
%%-------------------------------------------------------------------------------------------
-spec count_distinct(Data::[any()]) -> non_neg_integer().
count_distinct(Data) ->
    length(sets:to_list(sets:from_list(Data))).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Gets a list of pairs {Value, Count.
%% <br/><b>Data:</b> The list with the values.
%% @spec
%% count_by_value(Data::[any()]) -> [{any(),non_neg_integer()}]
%% @end
%%-------------------------------------------------------------------------------------------
-spec count_by_value(Data::[any()]) -> [{any(),non_neg_integer()}].
count_by_value(Data) ->
    Operation = fun(X, Map) -> maps:put(X, maps:get(X, Map, 0) + 1, Map) end,
    maps:to_list(lists:foldl(Operation, #{}, Data)).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Executes a task and obtains the result.
%% <br/><b>Task:</b> The task to execute.
%% @spec
%% execute(Task::#mrs_task{}) -> #mrs_task{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec execute(Task::#mrs_task{}) -> #mrs_task{}.
execute(Task) when is_record(Task, mrs_task) ->
    Step = Task#mrs_task.step,
    Chunk = Task#mrs_task.chunk,
    Mode = Step#mrs_step.mode,
    Params = Step#mrs_step.params,
    Task#mrs_task{chunk = execute(Mode, Params, Chunk)}.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Executes a task and obtains the result.
%% <br/><b>Mode:</b> The mode of the operation.
%% <br/><b>Params:</b> The parameters of the operation.
%% <br/><b>Data:</b> The data to process.
%% @spec
%% execute(Mode::any(),Params::any(),Data::any()) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec execute(Mode::any(),Params::any(),Data::any()) -> any().

%%****************************************
%% Parallelizable operations:
%%****************************************

execute(?FILTER_MODE, Operation, Data) ->
    %io:format("Filter => ~p~n", [Data]),
    [Item || Item <- Data, Operation(Item)];

execute(?FLATMAP_MODE, Operation, Data) ->
    %io:format("FlatMap() => ~p~n", [Data]),
    lists:append([Operation(Item) || Item <- Data]);

execute(?MAP_MODE, Operation, Data) ->
    %io:format("Map => ~p~n", [Data]),
    [Operation(Item) || Item <- Data];

execute(?REDUCEBYKEY_MODE, Operation, Data) ->
    %io:format("ReduceByKey() => ~p~n", [Data]),
    [{K, mrs_util:aggregate(Operation, V)} || {K, V} <- Data];

execute(?SORTBYKEY_MODE, true, Data) ->
   %io:format("SortByKey(Asc) => ~p~n", [Data]),
    [{K, lists:sort(get_asc_cmp(fun(X) -> X end), V)} || {K, V} <- Data];

execute(?SORTBYKEY_MODE, false, Data) ->
   %io:format("SortByKey(Desc) => ~p~n", [Data]),
    [{K, lists:sort(get_des_cmp(fun(X) -> X end), V)} || {K, V} <- Data];

execute(?ZIPWUID_MODE, _, Data) ->
    %io:format("ZipWithUID() => ~p~n", [Data]),
    [{make_ref(), Item} || Item <- Data];

execute(?AGGREGATEBYKEY_MODE, Operation, Data) ->
    %io:format("AggregateByKey() => ~p~n", [Data]),
    [{K, mrs_util:aggregate(Operation, V)} || {K, V} <- Data];

execute(?COUNTBYKEY_MODE, _, Data) ->
    %io:format("CountByKey() => ~p~n", [Data]),
    [{K, length(V)} || {K, V} <- Data];

execute(?COUNTDISTINCTBYKEY_MODE, _, Data) ->
    %io:format("CountDistinctByKey() => ~p~n", [Data]),
    [{K, count_distinct(V)} || {K, V} <- Data];

execute(?FOLDRBYKEY_MODE, {Initial, Operation}, Data) ->
    %io:format("FoldrByKey() => ~p~n", [Data]),
    [{K, lists:foldr(Operation, Initial, V)} || {K, V} <- Data];

execute(?FOLDLBYKEY_MODE, {Initial, Operation}, Data) ->
    %io:format("FoldlByKey() => ~p~n", [Data]),
    [{K, lists:foldl(Operation, Initial, V)} || {K, V} <- Data];

%%****************************************
%% Semi-parallelizable operations:
%%****************************************

execute(?GROUPBY_MODE, Operation, Data) ->
    %io:format("GroupBy() => ~p~n", [Data]),
    D2 = [{Operation(Item), Item} || Item <- Data],
    mrs_util:join_pairs(D2);

execute(?GROUPBYKEY_MODE, _, Data) ->
    %io:format("GroupByKey() => ~p~n", [Data]),
    mrs_util:join_pairs(Data);

execute(?REDUCE_MODE, Operation, Data) ->
    %io:format("Reduce() => ~p~n", [Data]),
    mrs_util:aggregate(Operation, Data);

%%****************************************
%% Non-parallelizable operations:
%%****************************************

execute(?SORTBY_MODE, {Selector, true}, Data) ->
    %io:format("SortBy(Asc) => ~p~n", [Data]),
    lists:sort(get_asc_cmp(Selector), Data);

execute(?SORTBY_MODE, {Selector, false}, Data) ->
    %io:format("SortBy(Desc) => ~p~n", [Data]),
    lists:sort(get_des_cmp(Selector), Data);

execute(?ZIP_MODE, OtherKey, Data) ->
    %io:format("Zip() => ~p~n", [Data]),
    case mrs_planner:get_query_result(OtherKey) of
        OtherData when is_list(OtherData) ->
            lists:zip(Data, OtherData);
        {?ERROR, Reasson} ->
            {?ERROR, Reasson};
        _ ->
            {?ERROR, invalid_other_data}
    end;

execute(?ZIPWIDX_MODE, _, Data) ->
    %io:format("ZipWithIndex() => ~p~n", [Data]),
    Indexes = lists:seq(1, length(Data)),
    lists:zip(Indexes, Data);

execute(?AGGREGATE_MODE, Operation, Data) ->
    %io:format("Aggregate() => ~p~n", [Data]),
    mrs_util:aggregate(Operation, Data);

execute(?COUNT_MODE, _, Data) ->
    %io:format("Count() => ~p~n", [Data]),
    length(Data);

execute(?COUNTDISTINCT_MODE, _, Data) ->
    %io:format("CountDistinct() => ~p~n", [Data]),
    count_distinct(Data);

execute(?COUNTBYVALUE_MODE, _, Data) ->
    %io:format("CountByValue() => ~p~n", [Data]),
    count_by_value(Data);

execute(?FOLDR_MODE, {Initial, Operation}, Data) ->
    %io:format("Foldr() => ~p~n", [Data]),
    lists:foldr(Operation, Initial, Data);

execute(?FOLDL_MODE, {Initial, Operation}, Data) ->
    %io:format("Foldl() => ~p~n", [Data]),
    lists:foldl(Operation, Initial, Data);

execute(?CARTESIAN_MODE, OtherKey, Data) ->
    %io:format("Cartesian() => ~p~n", [Data]),
    case mrs_planner:get_query_result(OtherKey) of
        OtherData when is_list(OtherData) ->
            [{D, O} || D <- Data, O <- OtherData];
        {?ERROR, Reasson} ->
            {?ERROR, Reasson};
        OtherElement ->
            [{D, OtherElement} || D <- Data]
    end;

execute(?DISTINCT_MODE, _, Data) ->
    %io:format("Distinct() => ~p~n", [Data]),
    sets:to_list(sets:from_list(Data));

execute(?INTERSECTION_MODE, OtherKey, Data) ->
    %io:format("Intersection() => ~p~n", [Data]),
    case mrs_planner:get_query_result(OtherKey) of
        OtherData when is_list(OtherData) ->
            [D || D <- Data, lists:member(D, OtherData)];
        {?ERROR, Reasson} ->
            {?ERROR, Reasson};
        OtherElement ->
            case lists:member(OtherElement, Data) of
                true -> [OtherElement];
                _ -> []
            end
    end;

execute(?SUBTRACT_MODE, OtherKey, Data) ->
    %io:format("Subtrack() => ~p~n", [Data]),
    case mrs_planner:get_query_result(OtherKey) of
        OtherData when is_list(OtherData) ->
            [D || D <- Data, not lists:member(D, OtherData)];
        {?ERROR, Reasson} ->
            {?ERROR, Reasson};
        OtherElement ->
            lists:filter(fun(Item) -> Item =/= OtherElement end, Data)
    end;

execute(?UNION_MODE, OtherKey, Data) ->
    %io:format("Union() => ~p~n", [Data]),
    case mrs_planner:get_query_result(OtherKey) of
        OtherData when is_list(OtherData) ->
            Data ++ OtherData;
        {?ERROR, Reasson} ->
            {?ERROR, Reasson};
        OtherElement ->
            [OtherElement | Data]
    end;

execute(?TAKE_MODE, Count, Data) ->
    %io:format("Take() => ~p~n", [Data]),
    lists:sublist(Data, 1, Count);

execute(?ISEMPTY_MODE, _, []) ->
    true;

execute(?ISEMPTY_MODE, _, _) ->
    false;

execute(_Mode, _Params, _Data) ->
    make_error(invalid_step_args).

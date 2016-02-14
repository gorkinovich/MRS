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
-module(mrs_api).
-author("Gorka Suárez García").
-include("mrs_api.hrl").
-include("mrs_job.hrl").
-export([
    create/1, create_from_file/1, collect/1, collect/2, save_as_text_file/2,
    save_as_text_file/3, filter/2, flat_map/2, group_by/2, group_by_key/1,
    map/2, reduce/2, reduce_by_key/2, sort_by/2, sort_by/3, sort_by_key/1,
    sort_by_key/2, zip/2, zip_with_index/1, zip_with_uid/1, aggregate/2,
    aggregate_by_key/2, count/1, count_by_key/1, count_distinct/1,
    count_distinct_by_key/1, count_by_value/1, foldl/3, foldl_by_key/3,
    foldr/3, foldr_by_key/3, cartesian/2, distinct/1, intersection/2,
    subtract/2, union/2, first/1, max/2, min/2, take/2, take_ordered/3,
    is_empty/1
]).

%%===========================================================================================
%% Create operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Creates a new query from a list of data.
%% <br/><b>Data:</b> The data of the query.
%% @spec
%% create(Data::maybe_improper_list()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec create(Data::maybe_improper_list()) -> #mrs_query{}.
create(Data) when is_list(Data) ->
    #mrs_query{data = Data, steps = queue:new()}.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Creates a new query from a file of data.
%% <br/><b>Path:</b> The path of the file.
%% @spec
%% create_from_file(Path::string()) -> #mrs_query{} | {'mrs:error',string()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec create_from_file(Path::string()) -> #mrs_query{} | {?ERROR,string()}.
create_from_file(Path) when is_list(Path) ->
    mrs_util:read_lines(Path, {?ERROR, "File read failed!"},
        fun(Lines) -> #mrs_query{data = Lines, steps = queue:new()} end).

%%===========================================================================================
%% Collect operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the result of a query execution.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% collect(Query::#mrs_query{}) -> [any()] | {'mrs:error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec collect(Query::#mrs_query{}) -> [any()] | {?ERROR,any()}.
collect(Query) when is_record(Query, mrs_query) ->
    generic_collect(fun(Q) -> mrs_planner:send_query(Q) end, Query).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the result of a query execution.
%% <br/><b>Node:</b> The node of the planner.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% collect(Node::atom(),Query::#mrs_query{}) -> [any()] | {'mrs:error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec collect(Node::atom(),Query::#mrs_query{}) -> [any()] | {?ERROR,any()}.
collect(Node, Query) when is_atom(Node), is_record(Query, mrs_query) ->
    generic_collect(fun(Q) -> mrs_planner:nsend_query(Node, Q) end, Query).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Gets the result of a query execution.
%% <br/><b>SendQuery:</b> The send query function.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% generic_collect(SendQuery::fun((#mrs_query{}) -> any()),Query::#mrs_query{}) ->
%%      [any()] | {'mrs:error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec generic_collect(SendQuery::fun((#mrs_query{}) -> any()),Query::#mrs_query{}) ->
    [any()] | {?ERROR,any()}.
generic_collect(SendQuery, Query) when is_function(SendQuery), is_record(Query, mrs_query) ->
    case SendQuery(Query) of
        ?JOB_FINISHED ->
            Query#mrs_query.data;
        {ok, Key} ->
            mrs_planner:receive_query(Key);
        {?ERROR, Reasson} ->
            {?ERROR, Reasson};
        _ ->
            {?ERROR, unknown}
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Saves the result of a query into a text file.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Path:</b> The output file path.
%% @spec
%% save_as_text_file(Query::#mrs_query{},Path::string()) -> 'ok' | {'mrs:error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec save_as_text_file(Query::#mrs_query{},Path::string()) -> 'ok' | {?ERROR,any()}.
save_as_text_file(Query, Path) when is_record(Query, mrs_query), is_list(Path) ->
    generic_save_as_text_file(fun(Q) -> collect(Q) end, Query, Path).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Saves the result of a query into a text file.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Path:</b> The output file path.
%% @spec
%% save_as_text_file(Node::atom(),Query::#mrs_query{}, Path::string()) -> 'ok'
%% @end
%%-------------------------------------------------------------------------------------------
-spec save_as_text_file(Node::atom(),Query::#mrs_query{},Path::string()) -> 'ok'.
save_as_text_file(Node, Query, Path) when is_atom(Node),
    is_record(Query, mrs_query), is_list(Path) ->
    generic_save_as_text_file(fun(Q) -> collect(Node, Q) end, Query, Path).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Saves the result of a query into a text file.
%% <br/><b>CollectQuery:</b> The collect query function.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Path:</b> The output file path.
%% @spec
%% generic_save_as_text_file(CollectQuery::fun((#mrs_query{}) -> any()),
%%      Query::#mrs_query{},Path::string()) -> 'ok' | {'mrs:error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec generic_save_as_text_file(CollectQuery::fun((#mrs_query{}) -> any()),
    Query::#mrs_query{},Path::string()) -> 'ok' | {?ERROR,any()}.
generic_save_as_text_file(CollectQuery, Query, Path)
    when is_record(Query, mrs_query), is_list(Path) ->
    case CollectQuery(Query) of
        {?ERROR, Reasson} ->
            {?ERROR, Reasson};
        Result ->
            mrs_util:write_lines(Path, Result)
    end.

%%===========================================================================================
%% Processing operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns a new collection containing only the elements that satisfy a predicate.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% filter(Query::#mrs_query{}, Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec filter(Query::#mrs_query{},Operation::fun()) -> #mrs_query{}.
filter(Query, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?FILTER_MODE, Operation).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Return a new collection by first applying a function to all elements of this collection,
%% and then flattening the results.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% flat_map(Query::#mrs_query{},Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec flat_map(Query::#mrs_query{},Operation::fun()) -> #mrs_query{}.
flat_map(Query, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?FLATMAP_MODE, Operation).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Return an collection of grouped items. Each group consists of a key and a sequence of
%% elements mapping to that key. The ordering of elements within each group is not
%% guaranteed, and may even differ each time the resulting collection is evaluated.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>GetKey:</b> The get key operation of the step.
%% @spec
%% group_by(Query::#mrs_query{}, Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec group_by(Query::#mrs_query{}, Operation::fun()) -> #mrs_query{}.
group_by(Query, GetKey) when is_record(Query, mrs_query), is_function(GetKey) ->
    add_new_step(Query, ?GROUPBY_MODE, GetKey).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Return an collection of grouped items. Each group consists of a key and a sequence of
%% elements mapping to that key. The ordering of elements within each group is not
%% guaranteed, and may even differ each time the resulting collection is evaluated.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% group_by_key(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec group_by_key(Query::#mrs_query{}) -> #mrs_query{}.
group_by_key(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?GROUPBYKEY_MODE).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns a new collection by applying a function to all elements of this collection.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% map(Query::#mrs_query{}, Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec map(Query::#mrs_query{},Operation::fun()) -> #mrs_query{}.
map(Query, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?MAP_MODE, Operation).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Reduces the elements of this collection using the specified commutative and
%% associative binary operator.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% reduce(Query::#mrs_query{}, Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec reduce(Query::#mrs_query{},Operation::fun()) -> #mrs_query{}.
reduce(Query, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?REDUCE_MODE, Operation).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Reduces the elements of this collection using the specified binary operator.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% reduce_by_key(Query::#mrs_query{}, Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec reduce_by_key(Query::#mrs_query{},Operation::fun()) -> #mrs_query{}.
reduce_by_key(Query, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?REDUCEBYKEY_MODE, Operation).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns this collection sorted by the given selector function.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Selector:</b> The selector of the step.
%% @spec
%% sort_by(Query::#mrs_query{}, Selector::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec sort_by(Query::#mrs_query{},Selector::fun()) -> #mrs_query{}.
sort_by(Query, Selector) when is_record(Query, mrs_query), is_function(Selector) ->
    add_new_step(Query, ?SORTBY_MODE, {Selector, true}).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns this collection sorted by the given selector function.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Selector:</b> The selector of the step.
%% <br/><b>Ascending:</b> The ascending sort flag.
%% @spec
%% sort_by(Query::#mrs_query{}, Selector::fun(), Ascending::boolean()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec sort_by(Query::#mrs_query{},Selector::fun(),Ascending::boolean()) -> #mrs_query{}.
sort_by(Query, Selector, Ascending) when is_record(Query, mrs_query), is_function(Selector) ->
    add_new_step(Query, ?SORTBY_MODE, {Selector, Ascending}).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns this collection sorted by the key of the value.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% sort_by_key(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec sort_by_key(Query::#mrs_query{}) -> #mrs_query{}.
sort_by_key(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?SORTBYKEY_MODE, true).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns this collection sorted by the key of the value.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Ascending:</b> The ascending sort flag.
%% @spec
%% sort_by_key(Query::#mrs_query{}, Ascending::boolean()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec sort_by_key(Query::#mrs_query{},Ascending::boolean()) -> #mrs_query{}.
sort_by_key(Query, Ascending) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?SORTBYKEY_MODE, Ascending).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Zips this collection with another one, returning pairs. Assumes that the two
%% collections have the same number of elements.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Other:</b> The other query in the operation.
%% @spec
%% zip(Query::#mrs_query{}, Other::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec zip(Query::#mrs_query{},Other::#mrs_query{}) -> #mrs_query{}.
zip(Query, Other) when is_record(Query, mrs_query), is_record(Other, mrs_query) ->
    add_new_step_woref(Query, ?ZIP_MODE, Other).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Zips this collection with its element indices.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% zip_with_index(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec zip_with_index(Query::#mrs_query{}) -> #mrs_query{}.
zip_with_index(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?ZIPWIDX_MODE).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Zips this collection with generated unique ids (erlang references).
%% <br/><b>Query:</b> The query to change.
%% @spec
%% zip_with_uid(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec zip_with_uid(Query::#mrs_query{}) -> #mrs_query{}.
zip_with_uid(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?ZIPWUID_MODE).

%%===========================================================================================
%% Reduction operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Aggregates the elements of this collection from left to right, using a given function.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% aggregate(Query::#mrs_query{}, Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec aggregate(Query::#mrs_query{},Operation::fun()) -> #mrs_query{}.
aggregate(Query, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?AGGREGATE_MODE, Operation).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Aggregates the elements of this collection from left to right, using a given function.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% aggregate_by_key(Query::#mrs_query{}, Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec aggregate_by_key(Query::#mrs_query{},Operation::fun()) -> #mrs_query{}.
aggregate_by_key(Query, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?AGGREGATEBYKEY_MODE, Operation).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the number of values in this collection.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% count(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec count(Query::#mrs_query{}) -> #mrs_query{}.
count(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?COUNT_MODE).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the number of values in this collection.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% count_by_key(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec count_by_key(Query::#mrs_query{}) -> #mrs_query{}.
count_by_key(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?COUNTBYKEY_MODE).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the number of distinct values in this collection.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% count_distinct(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec count_distinct(Query::#mrs_query{}) -> #mrs_query{}.
count_distinct(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?COUNTDISTINCT_MODE).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the number of distinct values in this collection.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% count_distinct_by_key(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec count_distinct_by_key(Query::#mrs_query{}) -> #mrs_query{}.
count_distinct_by_key(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?COUNTDISTINCTBYKEY_MODE).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the count of each unique value in this collection as a local map of
%% (value, count) pairs.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% count_by_value(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec count_by_value(Query::#mrs_query{}) -> #mrs_query{}.
count_by_value(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?COUNTBYVALUE_MODE).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Aggregates the elements of this collection from left to right, using a given
%% function and a neutral "zero value".
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Initial:</b> The initial value of the step.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% foldl(Query::#mrs_query{}, Initial::any(), Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec foldl(Query::#mrs_query{},Initial::any(),Operation::fun()) -> #mrs_query{}.
foldl(Query, Initial, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?FOLDL_MODE, {Initial, Operation}).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Aggregates the elements of this collection from left to right, using a given
%% function and a neutral "zero value".
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Initial:</b> The initial value of the step.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% foldl_by_key(Query::#mrs_query{}, Initial::any(), Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec foldl_by_key(Query::#mrs_query{},Initial::any(),Operation::fun()) -> #mrs_query{}.
foldl_by_key(Query, Initial, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?FOLDLBYKEY_MODE, {Initial, Operation}).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Aggregates the elements of this collection from right to left, using a given
%% function and a neutral "zero value".
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Initial:</b> The initial value of the step.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% foldr(Query::#mrs_query{}, Initial::any(), Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec foldr(Query::#mrs_query{},Initial::any(),Operation::fun()) -> #mrs_query{}.
foldr(Query, Initial, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?FOLDR_MODE, {Initial, Operation}).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Aggregates the elements of this collection from right to left, using a given
%% function and a neutral "zero value".
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Initial:</b> The initial value of the step.
%% <br/><b>Operation:</b> The operation of the step.
%% @spec
%% foldr_by_key(Query::#mrs_query{}, Initial::any(), Operation::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec foldr_by_key(Query::#mrs_query{},Initial::any(),Operation::fun()) -> #mrs_query{}.
foldr_by_key(Query, Initial, Operation) when is_record(Query, mrs_query), is_function(Operation) ->
    add_new_step(Query, ?FOLDRBYKEY_MODE, {Initial, Operation}).

%%===========================================================================================
%% Set operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the cartesian product of this collection and another one, that is, the collection
%% of all pairs of elements (a, b) where a is in this and b is in other.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Other:</b> The other query in the operation.
%% @spec
%% cartesian(Query::#mrs_query{},Other::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec cartesian(Query::#mrs_query{},Other::#mrs_query{}) -> #mrs_query{}.
cartesian(Query, Other) when is_record(Query, mrs_query), is_record(Other, mrs_query) ->
    add_new_step_woref(Query, ?CARTESIAN_MODE, Other).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns a new collection containing the distinct elements in this collection.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% distinct(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec distinct(Query::#mrs_query{}) -> #mrs_query{}.
distinct(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?DISTINCT_MODE).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the intersection of this collection and another one. The output will not contain
%% any duplicate elements, even if the input collections did.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Other:</b> The other query in the operation.
%% @spec
%% intersection(Query::#mrs_query{},Other::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec intersection(Query::#mrs_query{},Other::#mrs_query{}) -> #mrs_query{}.
intersection(Query, Other) when is_record(Query, mrs_query), is_record(Other, mrs_query) ->
    add_new_step_woref(Query, ?INTERSECTION_MODE, Other).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns an collection with the elements from this that are not in other.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Other:</b> The other query in the operation.
%% @spec
%% subtract(Query::#mrs_query{}, Other::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec subtract(Query::#mrs_query{},Other::#mrs_query{}) -> #mrs_query{}.
subtract(Query, Other) when is_record(Query, mrs_query), is_record(Other, mrs_query) ->
    add_new_step_woref(Query, ?SUBTRACT_MODE, Other).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the union of this collection and another one. Any identical elements will appear
%% multiple times (use distinct() to eliminate them).
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Other:</b> The other query in the operation.
%% @spec
%% union(Query::#mrs_query{}, Other::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec union(Query::#mrs_query{},Other::#mrs_query{}) -> #mrs_query{}.
union(Query, Other) when is_record(Query, mrs_query), is_record(Other, mrs_query) ->
    add_new_step_woref(Query, ?UNION_MODE, Other).

%%===========================================================================================
%% Take operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the first element of this collection.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% first(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec first(Query::#mrs_query{}) -> #mrs_query{}.
first(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?TAKE_MODE, 1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the max element of this collection sorted by the given selector function.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Selector:</b> The selector of the step.
%% @spec
%% max(Query::#mrs_query{}, Selector::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec max(Query::#mrs_query{},Selector::fun()) -> #mrs_query{}.
max(Query, Selector) when is_record(Query, mrs_query) ->
    add_new_step(add_new_step(Query, ?SORTBY_MODE, {Selector, false}), ?TAKE_MODE, 1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the min element of this collection sorted by the given selector function.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Selector:</b> The selector of the step.
%% @spec
%% min(Query::#mrs_query{}, Selector::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec min(Query::#mrs_query{},Selector::fun()) -> #mrs_query{}.
min(Query, Selector) when is_record(Query, mrs_query) ->
    add_new_step(add_new_step(Query, ?SORTBY_MODE, {Selector, true}), ?TAKE_MODE, 1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the first k elements from this collection.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Count:</b> The number of elements to take.
%% @spec
%% take(Query::#mrs_query{}, Count::number()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec take(Query::#mrs_query{},Count::number()) -> #mrs_query{}.
take(Query, Count) when is_record(Query, mrs_query),
    is_number(Count), Count > 0 ->
    add_new_step(Query, ?TAKE_MODE, Count).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns the first k elements from this collection as defined by the specified selector
%% and maintains the ordering.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Count:</b> The number of elements to take.
%% <br/><b>Selector:</b> The selector of the step.
%% @spec
%% take_ordered(Query::#mrs_query{}, Count::number(), Selector::fun()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec take_ordered(Query::#mrs_query{},Count::number(),Selector::fun()) -> #mrs_query{}.
take_ordered(Query, Count, Selector) when is_record(Query, mrs_query),
    is_number(Count), Count > 0, is_function(Selector) ->
    add_new_step(add_new_step(Query, ?SORTBY_MODE, {Selector, true}), ?TAKE_MODE, Count).

%%===========================================================================================
%% Other operations
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Returns true if and only if the collection contains no elements at all.
%% <br/><b>Query:</b> The query to change.
%% @spec
%% is_empty(Query::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec is_empty(Query::#mrs_query{}) -> #mrs_query{}.
is_empty(Query) when is_record(Query, mrs_query) ->
    add_new_step(Query, ?ISEMPTY_MODE).

%%===========================================================================================
%% Utility functions
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Adds a new step inside a query.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Mode:</b> The operation mode in the step.
%% @spec
%% add_new_step(Query::#mrs_query{},Mode::atom()) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec add_new_step(Query::#mrs_query{},Mode::atom()) -> #mrs_query{}.
add_new_step(Query, Mode) when is_record(Query, mrs_query), is_atom(Mode) ->
    add_step(Query, #mrs_step{mode = Mode, params = ?UNDEFINED}).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Adds a new step inside a query.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Mode:</b> The operation mode in the step.
%% <br/><b>Params:</b> The parameters of the step.
%% @spec
%% add_new_step(Query::#mrs_query{}, Mode::atom(), Params::fun() |
%%      number() | {any(),fun()} | {fun(),boolean()}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec add_new_step(Query::#mrs_query{},Mode::atom(),Params::fun() |
    number() | {any(),fun()} | {fun(),boolean()}) -> #mrs_query{}.
add_new_step(Query, Mode, Params) when is_record(Query, mrs_query), is_atom(Mode) ->
    add_step(Query, #mrs_step{mode = Mode, params = Params}).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Adds a new step inside a query.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Mode:</b> The operation mode in the step.
%% <br/><b>Other:</b> The other query in the operation.
%% @spec
%% add_new_step_woref(Query::#mrs_query{},Mode::atom(),
%%      Other::#mrs_query{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec add_new_step_woref(Query::#mrs_query{}, Mode::atom(),
    Other::#mrs_query{}) -> #mrs_query{}.
add_new_step_woref(Query, Mode, Other) when is_record(Query, mrs_query),
    is_atom(Mode), is_record(Other, mrs_query) ->
    add_step(Query, #mrs_step{mode = Mode, params = Other#mrs_query.ref}).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Adds a new step inside a query.
%% <br/><b>Query:</b> The query to change.
%% <br/><b>Step:</b> The step data to add.
%% @spec
%% add_step(Query::#mrs_query{}, Step::#mrs_step{}) -> #mrs_query{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec add_step(Query::#mrs_query{}, Step::#mrs_step{}) -> #mrs_query{}.
add_step(Query, Step) when is_record(Query, mrs_query), is_record(Step, mrs_step) ->
    NextSteps = queue:in(Step, Query#mrs_query.steps),
    Query#mrs_query{steps = NextSteps}.

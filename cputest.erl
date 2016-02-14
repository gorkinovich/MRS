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
-module(cputest).
-author("Gorka Suárez García").
-export([
    get_optimal_processes_number/0, generate_test_data/1, multicore_sort/2, is_sorted/1
]).

%%===========================================================================================
%% Data
%%===========================================================================================

-define(TEST_DATA_SIZE, 500000).

%%===========================================================================================
%% Optimal processes number
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Calculates the best number of processes to use with the current machine.
%% @spec
%% get_optimal_processes_number() -> pos_integer()
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_optimal_processes_number() -> pos_integer().
get_optimal_processes_number() ->
    Victims = generate_test_data(?TEST_DATA_SIZE),
    Time = measure_sort_time(Victims, 1),
    get_optimal_processes_number(Time, 1, Victims, 2).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Calculates the best number of processes to use with the current machine.
%% <br/><b>BestTime:</b> The current best time.
%% <br/><b>BestProcs:</b> The number of processes used in the best time.
%% <br/><b>Victims:</b> The generated data to do the tests.
%% <br/><b>NumProcs:</b> The current number of processes to do the test.
%% @spec
%% get_optimal_processes_number(BestTime::integer(),
%%      BestProcs::pos_integer(), Victims::[pos_integer()],
%%      NumProcs::pos_integer()) -> pos_integer()
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_optimal_processes_number(BestTime::integer(),BestProcs::pos_integer(),
    Victims::[pos_integer()],NumProcs::pos_integer()) -> pos_integer().
get_optimal_processes_number(BestTime, BestProcs, Victims, NumProcs) ->
    Time = measure_sort_time(Victims, NumProcs),
    case Time > BestTime of
        true -> BestProcs;
        _ -> get_optimal_processes_number(Time, NumProcs, Victims, NumProcs * 2)
    end.

%%===========================================================================================
%% Multicore sort
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sorts a list of numbers, using 1 or more processes.
%% <br/><b>Data:</b> The list to sort.
%% <br/><b>MaxProcs:</b> The maximum number of processes allowed.
%% @spec
%% multicore_sort(Data::[any()], MaxProcs::number()) -> maybe_improper_list()
%% @end
%%-------------------------------------------------------------------------------------------
-spec multicore_sort(Data::[any()],MaxProcs::number()) -> maybe_improper_list().
multicore_sort(Data, MaxProcs) when is_list(Data), is_number(MaxProcs), MaxProcs > 0 ->
    multicore_sort(Data, 1, MaxProcs).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Sorts a list of numbers, using 1 or more processes.
%% <br/><b>Data:</b> The list to sort.
%% <br/><b>CurProcs:</b> The current number of processes.
%% <br/><b>MaxProcs:</b> The maximum number of processes allowed.
%% @spec
%% multicore_sort(Data::[any()], CurProcs::pos_integer(),
%%      MaxProcs::number()) -> maybe_improper_list()
%% @end
%%-------------------------------------------------------------------------------------------
-spec multicore_sort(Data::[any()],CurProcs::pos_integer(),
    MaxProcs::number()) -> maybe_improper_list().
multicore_sort([], _, _) ->
    [];
multicore_sort([D|DS], CurProcs, MaxProcs) ->
    LUDS = [X || X <- DS, X < D],
    RUDS = [X || X <- DS, X >= D],
    case CurProcs < MaxProcs of
        true ->
            % Only if we haven't reached the maximum number of processes allowed,
            % we'll make 2 new processes to distribute the work of the operation.
            This = self(),
            NextCall = fun (Data) ->
                spawn_link(fun () ->
                    This ! {self(), multicore_sort(Data, CurProcs * 2, MaxProcs)}
                           end)
                       end,
            GetValue = fun (PID) ->
                receive {PID, Value} -> Value end
                       end,
            % Here we'll invoke the next steps, getting the PIDs of the processes,
            % and after that we'll get the results with a receive.
            LPID = NextCall(LUDS), RPID = NextCall(RUDS),
            LSDS = GetValue(LPID), RSDS = GetValue(RPID),
            LSDS ++ [D] ++ RSDS;
        _ ->
            % If we have reached the maximum number of processes allowed, we'll
            % only call the next step of the sort and join the results.
            LSDS = multicore_sort(LUDS, CurProcs, MaxProcs),
            RSDS = multicore_sort(RUDS, CurProcs, MaxProcs),
            LSDS ++ [D] ++ RSDS
    end.

%%===========================================================================================
%% Is sorted
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Checks if a list of elements is sorted or not.
%% <br/><b>List:</b> The list to check.
%% @spec
%% is_sorted(List::maybe_improper_list()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec is_sorted(List::maybe_improper_list()) -> boolean().
is_sorted([]) ->
    true;
is_sorted([_]) ->
    true;
is_sorted([A,B|L]) ->
    case A > B of
        true -> false;
        _ -> is_sorted([B|L])
    end.

%%===========================================================================================
%% Utility functions
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Generates a list of random numbers.
%% <br/><b>Size:</b> The size of the list to generate.
%% @spec
%% generate_test_data(Size::pos_integer()) -> [pos_integer()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec generate_test_data(Size::pos_integer()) -> [pos_integer()].
generate_test_data(Size) when is_number(Size), Size > 0 ->
    [random:uniform(Size) || _ <- lists:seq(1, Size)].

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Measures the used time to sorts a list of numbers.
%% <br/><b>Data:</b> The list to sort.
%% <br/><b>MaxProcs:</b> The maximum number of processes allowed.
%% @spec
%% measure_sort_time(Data::[pos_integer()], MaxProcs::pos_integer()) -> integer()
%% @end
%%-------------------------------------------------------------------------------------------
-spec measure_sort_time(Data::[pos_integer()],MaxProcs::pos_integer()) -> integer().
measure_sort_time(Data, MaxProcs) when is_list(Data), is_number(MaxProcs), MaxProcs > 0 ->
    {Time, _} = timer:tc(?MODULE, multicore_sort, [Data, MaxProcs]),
    Time.

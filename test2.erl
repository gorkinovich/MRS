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
-module(test2).
-author("Gorka Suárez García").
-include("mrs_api.hrl").
-compile(export_all).

% c(test2),c(mrs_api),c(mrs_db),c(mrs_jlogic),c(mrs_job),c(mrs_jtask),c(mrs_planner),c(mrs_util),c(mrs_worker).
% test2:start().
% test2:start(), test2:example().
% test2:start(), test2:example1().
% test2:start(), test2:example2().
% test2:start(), test2:example3().
% test2:start(), test2:example4().

start() ->
    mrs_planner:start().

get_path(RelPath) ->
    {ok, CWD} = file:get_cwd(),
    filename:join(CWD, RelPath).

example() ->
    Data = [{madrid,34}, {barcelona,21}, {madrid,22}, {barcelona,19}, {teruel,-5},
        {teruel, 14}, {madrid,37}, {teruel, -8}, {barcelona,30}, {teruel,10}],
    Q0 = mrs_api:create(Data),
    Q1 = mrs_api:filter(Q0, fun({_,X})-> X > 28 end),
    Q2 = mrs_api:group_by_key(Q1),
    Q3 = mrs_api:sort_by_key(Q2, false),
    Q4 = mrs_api:map(Q3, fun({K,V})-> {K, mrs_util:get_head(V)} end),
    R = mrs_api:collect(Q4),
    io:format("~n~nQuery result:~n~w~n~n", [R]).

example1() ->
    Seps = " ,;:.?!\"'()+-/*",
    Path = get_path("./data/hamlet.txt"),
    Q0 = mrs_api:create_from_file(Path),
    Q1 = mrs_api:flat_map(Q0, fun(X)-> string:tokens(X, Seps) end),
    Q2 = mrs_api:map(Q1, fun(X)-> [I || I <- X, I < 128] end),
    Q3 = mrs_api:map(Q2, fun(X)-> {X, 1} end),
    Q4 = mrs_api:group_by_key(Q3),
    Q5 = mrs_api:reduce_by_key(Q4, fun(A,B)-> A + B end),
    Q6 = mrs_api:sort_by(Q5, fun({_,X})-> X end, false),
    Q7 = mrs_api:take(Q6, 100),
    R = mrs_api:collect(Q7),
    %mrs_util:write_lines(Path ++ ".out", R),
    io:format("Query result: ~p~n", [R]).

example2() ->
    Path = get_path("./data/weblog.txt"),
    Q0 = mrs_api:create_from_file(Path),
    Q1 = mrs_api:map(Q0, fun(X)-> string:tokens(X, " ") end),
    Q2 = mrs_api:filter(Q1, fun(X)-> lists:nth(length(X) - 1, X) =:= "302" end),
    Q3 = mrs_api:map(Q2, fun(X)-> V = lists:nth(2, X),
        {string:sub_string(V, 2, 3), 1} end),
    Q4 = mrs_api:group_by_key(Q3),
    Q5 = mrs_api:reduce_by_key(Q4, fun(A,B)-> A + B end),
    Q6 = mrs_api:sort_by(Q5, fun({X,_})-> X end, false),
    R = mrs_api:collect(Q6),
    %mrs_util:write_lines(Path ++ ".out", R),
    io:format("Query result: ~p~n", [R]).

example3() ->
    Path = get_path("./data/temperature.txt"),
    Q0 = mrs_api:create_from_file(Path),
    Q1 = mrs_api:map(Q0, fun(X)-> string:tokens(X, ",") end),
    Q2 = mrs_api:filter(Q1, fun(X)-> X =/= [] end),
    Q3 = mrs_api:map(Q2, fun(X)->
        try
            F2 = mrs_util:to_integer(lists:nth(2, X)),
            F3 = mrs_util:to_integer(lists:nth(3, X)),
            F8 = mrs_util:to_float(lists:nth(8, X)),
            F13 = mrs_util:to_float(lists:nth(13, X)),
            {F2 * 1000 + F3, F8 - F13}
        catch
            _:_ -> {0, 0.0}
        end end),
    Q4 = mrs_api:group_by_key(Q3),
    Q5 = mrs_api:map(Q4, fun({K, V})-> {K, lists:max(V), lists:min(V)} end),
    Q6 = mrs_api:sort_by(Q5, fun({X,_,_})-> X end, false),
    Q7 = mrs_api:map(Q6, fun({A,B,C})-> {trunc(A / 1000),A rem 1000,B,C} end),
    R = mrs_api:collect(Q7),
    %mrs_util:write_lines(Path ++ ".out", R),
    io:format("Query result: ~p~n", [R]),
    ok.

example4() ->
    Path = get_path("./data/happiness.txt"),
    Q0 = mrs_api:create_from_file(Path),
    Q1 = mrs_api:map(Q0, fun(X)-> string:tokens(X, "\t") end),
    Q2 = mrs_api:filter(Q1, fun(X)-> X =/= [] end),
    Q3 = mrs_api:filter(Q2, fun(X)->
        F3 = mrs_util:to_float(lists:nth(3, X)),
        F5 = lists:nth(5, X),
        F3 < 2.0 andalso F5 =/= "--" end),
    Q4 = mrs_api:map(Q3, fun(X)-> {"Palabras muy tristes", {lists:nth(1, X)}} end),
    Q5 = mrs_api:group_by_key(Q4),
    Q6 = mrs_api:foldl_by_key(Q5, "", fun({I},S)-> S ++ ", " ++ I end),
    Q7 = mrs_api:map(Q6, fun({K,V})-> K ++ ": " ++ V end),
    R = mrs_api:collect(Q7),
    %mrs_util:write_lines(Path ++ ".out", R),
    io:format("Query result: ~p~n", [R]).

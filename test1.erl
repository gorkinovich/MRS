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
-module(test1).
-author("Gorka Suárez García").
-include("mrs_api.hrl").
-compile(export_all).

% c(test1),c(mrs_api),c(mapredsrv),c(mapredwrk),c(mrs_util).
% test1:example1().
% test1:example2().

example1() ->
    Data = [{madrid,34}, {barcelona,21}, {madrid,22}, {barcelona,19}, {teruel,-5},
        {teruel, 14}, {madrid,37}, {teruel, -8}, {barcelona,30}, {teruel,10}],
    Fmap =
        fun({C, T}) ->
            io:format("Map => ~w~n", [{C, T}]),
            case T > 28 of
                true -> {C, T};
                _ -> nothing
            end
        end,
    Freduce =
        fun({C, TS}) ->
            io:format("Reduce => ~w~n", [{C, TS}]),
            {C, lists:max(TS)}
        end,
    mapredsrv:start(Data, 3),
    mapredsrv:send_map_reduce(Fmap, Freduce),
    receive
        {_, Result} ->
            io:format("Result:~n~w~n", [Result])
    end.

example2() ->
    Data = [
        "This was a triumph!", "I'm making a note here:", "\"Huge success!!\"",
        "It's hard to overstate", "My satisfaction.", "Aperture science:", "We do what me must",
        "Because we can.", "For the good of all of us.", "Except the ones who are dead.",
        "But there's no sense crying", "Over every mistake.", "You just keep on trying",
        "Till you run out of cake.", "And the science gets done.", "And you make a neat gun",
        "For the people who are", "Still alive.", "I'm not even angry...",
        "I'm being so sincere right now-", "Even though you broke my heart,",
        "And killed me.", "And tore me to pieces.", "And threw every piece into a fire.",
        "As they burned it hurt because", "I was so happy for you!", "Now, these points of data",
        "Make a beautiful line.", "And we're out of beta.", "We're releasing on time!",
        "So I'm glad i got burned-", "Think of all the things we learned-",
        "For the people who are", "Still alive.", "Go ahead and leave me...",
        "I think I'd prefer to stay inside...", "Maybe you'll find someone else",
        "To help you?", "Maybe black mesa?", "That was a joke! ha ha!! fat chance!!",
        "Anyway this cake is great!", "It's so delicious and moist!", "Look at me: still talking",
        "When there's science to do!", "When I look out there,", "'It makes me glad I'm not you!'",
        "I've experiments to run.", "There is research to be done.", "On the people who are",
        "Still alive.", "And believe me I am", "Still alive.", "I'm doing science and I'm",
        "Still alive.", "I feel fantastic and I'm", "Still alive.",
        "While you're dying I'll be", "Still alive.", "And when you're dead I will be",
        "Still alive.", "Still alive.", "Still alive.", "(End Of Song)"
    ],
    Fmap =
        fun(X) ->
            io:format("Map => ~p~n", [X]),
            Seps = " ,;:.?!\"()+-/*",
            [{V, 1} || V <- string:tokens(X, Seps), V =/= []]
        end,
    Freduce =
        fun({C, TS}) ->
            io:format("Reduce => ~p~n", [{C, TS}]),
            {C, lists:foldl(fun(E, AS) -> E + AS end, 0, TS)}
        end,
    mapredsrv:start(Data, 4),
    mapredsrv:send_map_reduce(Fmap, Freduce),
    receive
        {_, Result} ->
            io:format("Result:~n~p~n", [Result])
    end.

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
-module(mrs_util).
-author("Gorka Suárez García").
-export([
    is_natural/1, is_positive/1, to_integer/1, to_float/1, setelement/4, isEmpty/1,
    isNotEmpty/1, get_head/1, get_heads/1, join/2, append/2, join_pairs/1, aggregate/2,
    queue_out/2, list_to_queue/1, ref_to_atom/1, node_name/0, node_host/0, node_name/1,
    node_host/1, find_node_names/0, find_node_names/1, find_nodes/0, find_nodes/1,
    find_nodes_from_hosts/1, find_other_nodes_from_hosts/1, read_lines/2, read_lines/3,
    write_lines/2, open_utf8/2, open_utf8_to_read/1, open_utf8_to_write/1, get_lines/1,
    put_lines/2, if_then/2
]).

%%===========================================================================================
%% Numbers
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Checks if a number is a natural number (N >= 0).
%% <br/><b>V:</b> The value to check.
%% @spec
%% is_natural(V::any()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec is_natural(V::any()) -> boolean().
is_natural(V) when is_number(V), V >= 0 -> true;
is_natural(_) -> false.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Checks if a number is a positive natural number (N > 0).
%% <br/><b>V:</b> The value to check.
%% @spec
%% is_positive(V::any()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec is_positive(V::any()) -> boolean().
is_positive(V) when is_number(V), V > 0 -> true;
is_positive(_) -> false.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Converts a value into an integer.
%% <br/><b>V:</b> The value to convert.
%% @spec
%% to_integer(V::any()) -> integer()
%% @end
%%-------------------------------------------------------------------------------------------
-spec to_integer(V::any()) -> integer().
to_integer([]) ->
    0;
to_integer(V) when is_number(V) ->
    V;
to_integer(V) when is_float(V) ->
    trunc(V);
to_integer(V) when is_list(V) ->
    try
        list_to_integer(V)
    catch _:_ ->
        0
    end;
to_integer(_) ->
    0.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Converts a value into a float.
%% <br/><b>V:</b> The value to convert.
%% @spec
%% to_float(V::any()) -> float()
%% @end
%%-------------------------------------------------------------------------------------------
-spec to_float(V::any()) -> float().
to_float([]) ->
    float(0);
to_float(V) when is_float(V) ->
    V;
to_float(V) when is_number(V) ->
    float(V);
to_float(V) when is_list(V) ->
    try
        list_to_float(V)
    catch _:_ ->
        float(to_integer(V))
    end;
to_float(_) ->
    float(0).

%%===========================================================================================
%% Tuples
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets an element inside a tuple if the predicate is true.
%% <br/><b>Index:</b> The index of the field.
%% <br/><b>Tuple:</b> The tuple to change.
%% <br/><b>Value:</b> The value to set.
%% <br/><b>Predicate:</b> The test predicate.
%% @spec
%% setelement(Index::number(),Tuple::tuple(),Value::any(),
%%      Predicate::fun((any()) -> any())) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec setelement(Index::number(),Tuple::tuple(),Value::any(),
    Predicate::fun((any()) -> any())) -> tuple().
setelement(Index, Tuple, Value, Predicate) when is_tuple(Tuple),
    is_number(Index), Index > 0, Index =< size(Tuple) ->
    case Predicate(Value) of
        true -> setelement(Index, Tuple, Value);
        _ -> Tuple
    end.

%%===========================================================================================
%% Lists
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Checks if a list is empty (it also returns true if it isn't a list).
%% <br/><b>V:</b> The value with the list.
%% @spec
%% isEmpty(V::any()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec isEmpty(V::any()) -> boolean().
isEmpty(V) when is_list(V) -> length(V) =< 0;
isEmpty(_) -> true.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Checks if a list isn't empty (it also returns false if it isn't a list).
%% <br/><b>V:</b> The value with the list.
%% @spec
%% isNotEmpty(V::any()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec isNotEmpty(V::any()) -> boolean().
isNotEmpty(V) when is_list(V) -> length(V) > 0;
isNotEmpty(_) -> false.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the head in a list, if it isn't a list returns the value itself.
%% <br/><b>V:</b> The value with the list.
%% @spec
%% get_head(V::any()) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_head(V::any()) -> any().
get_head(V) when is_list(V) -> hd(V);
get_head(V) -> V.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the heads in a list of lists. Those elements in the list, that aren't lists or
%% are empty, will not be included in the final result.
%% <br/><b>VS:</b> The list of values.
%% @spec
%% get_heads(VS::[any()]) -> [any()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_heads(VS::[any()]) -> [any()].
get_heads(VS) when is_list(VS) ->
    [get_head(V) || V <- VS, (not is_list(V)) orelse isNotEmpty(V)].

%%-------------------------------------------------------------------------------------------
%% @doc
%% Joins a couple of elements into one list.
%% <br/><b>Left:</b> The left side element.
%% <br/><b>Right:</b> The right side element.
%% @spec
%% join(Left::any(), Right::any()) -> [any()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec join(Left::any(), Right::any()) -> [any()].
join(L1, L2) when is_list(L1), is_list(L2) -> L1 ++ L2;
join(L1, E2) when is_list(L1) -> L1 ++ [E2];
join(E1, L2) when is_list(L2) -> [E1|L2];
join(E1, E2) -> [E1, E2].

%%-------------------------------------------------------------------------------------------
%% @doc
%% Appends an element into a list.
%% <br/><b>List:</b> The list of elements.
%% <br/><b>Element:</b> The value to append.
%% @spec
%% append(List::any(), Element::any()) -> [any()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec append(List::any(), Element::any()) -> [any()].
append(L1, L2) when is_list(L1), is_list(L2) -> L1 ++ [L2];
append(L1, E2) when is_list(L1) -> L1 ++ [E2];
append(E1, L2) when is_list(L2) -> [E1] ++ [L2];
append(E1, E2) -> [E1, E2].

%%-------------------------------------------------------------------------------------------
%% @doc
%% Joins a list of pairs {key, value}.
%% <br/><b>Data:</b> The list with the pairs.
%% @spec
%% join_pairs(Data::[tuple()]) -> [tuple()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec join_pairs(Data::[tuple()]) -> [tuple()].
join_pairs([]) ->
    [];
join_pairs(Data) ->
    SD = lists:keysort(1, Data),
    D2 = join_pairs([], hd(SD), tl(SD)),
    MakeList =
        fun(X) ->
            if
                is_list(X) -> X;
                true -> [X]
            end
        end,
    [{K, MakeList(V)} || {K, V} <- D2].

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Joins a sorted list of pairs {key, value}.
%% <br/><b>RD:</b> The accumulated reversed data.
%% <br/><b>PV:</b> The current selected pair.
%% <br/><b>DS:</b> The sorted list with the next elements.
%% @spec
%% join_pairs(RD::[{Key::any(),Value::any()}],PV::{Key::any(),Value::any()},
%%      DS::[{Key::any(),Value::any()}]) -> [{Key::any(),Value::any()}]
%% @end
%%-------------------------------------------------------------------------------------------
-spec join_pairs(RD::[{Key::any(),Value::any()}],PV::{Key::any(),Value::any()},
    DS::[{Key::any(),Value::any()}]) -> [{Key::any(),Value::any()}].
join_pairs(RD, PV, []) ->
    lists:reverse([PV|RD]);
join_pairs(RD, {K, VS}, [{K, IVS}|IDS]) ->
    join_pairs(RD, {K, join(VS, IVS)}, IDS);
join_pairs(RD, {K, VS}, [{IK, IVS}|IDS]) ->
    join_pairs([{K, VS}|RD], {IK, IVS}, IDS).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Reduce a list using the specified binary operator.
%% <br/><b>Operation:</b> The reduce operation.
%% <br/><b>Data:</b> The list of values.
%% @spec
%% aggregate(Operation::fun((Item::any(),Acc::any()) -> any()),Data::[any()]) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec aggregate(Operation::fun((Item::any(),Acc::any()) -> any()),Data::[any()]) -> any().
aggregate(_, []) ->
    [];
aggregate(Operation, [D|DS]) ->
    lists:foldl(Operation, D, DS).

%%===========================================================================================
%% Queues
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the first element in the queue or a default element.
%% <br/><b>Queue:</b> The queue with the values.
%% <br/><b>DefaultValue:</b> The default value if the queue is empty.
%% @spec
%% queue_out(Queue::queue:queue(any()),DefaultValue::any()) ->
%%      {any(),queue:queue(any())}
%% @end
%%-------------------------------------------------------------------------------------------
-spec queue_out(Queue::queue:queue(any()),DefaultValue::any()) ->
    {any(),queue:queue(any())}.
queue_out(Queue, DefaultValue) ->
    case queue:out(Queue) of
        {{value, S}, SQ} -> {S, SQ};
        {_, SQ} -> {DefaultValue, SQ};
        _ -> {DefaultValue, queue:new()}
    end.

%%===========================================================================================
%% Transform
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Converts a list into a queue.
%% <br/><b>VS:</b> The list of values.
%% @spec
%% list_to_queue(VS::[any()]) -> queue:queue(any())
%% @end
%%-------------------------------------------------------------------------------------------
-spec list_to_queue(VS::[any()]) -> queue:queue(any()).
list_to_queue(VS) when is_list(VS) ->
    lists:foldl(fun(V, Q) -> queue:in(V, Q) end, queue:new(), VS).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Converts a reference into an atom.
%% <br/><b>R:</b> The reference value.
%% @spec
%% ref_to_atom(R::reference()) -> atom()
%% @end
%%-------------------------------------------------------------------------------------------
-spec ref_to_atom(R::reference()) -> atom().
ref_to_atom(R) when is_reference(R) ->
    list_to_atom(erlang:ref_to_list(R)).

%%===========================================================================================
%% Nodes
%%===========================================================================================

% start /B werl -name N1@192.168.0.1 -setcookie sex
% node()                            -> 'N1@192.168.0.1'
% nodes(connected)                  -> ['N2@192.168.0.2']
% net_adm:ping('N2@192.168.0.2')    -> pong | pang
% net_adm:names("localhost")        -> {ok,[{"N1",1234}]}
% net_adm:names("192.168.0.2")      -> {ok,[{"N2",4321}]}

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the name of the current node.
%% @spec
%% node_name() -> string()
%% @end
%%-------------------------------------------------------------------------------------------
-spec node_name() -> string().
node_name() ->
    node_name(node()).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the host of the current node.
%% @spec
%% node_host() -> string()
%% @end
%%-------------------------------------------------------------------------------------------
-spec node_host() -> string().
node_host() ->
    node_host(node()).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the name of a node.
%% <br/><b>N:</b> The atom with the node.
%% @spec
%% node_name(N::atom()) -> string()
%% @end
%%-------------------------------------------------------------------------------------------
-spec node_name(N::atom()) -> string().
node_name(N) when is_atom(N) ->
    TS = string:tokens(atom_to_list(N), "@"),
    case length(TS) > 1 of
        true -> hd(TS);
        _ -> ""
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the host of a node.
%% <br/><b>N:</b> The atom with the node.
%% @spec
%% node_host(N::atom()) -> string()
%% @end
%%-------------------------------------------------------------------------------------------
-spec node_host(N::atom()) -> string().
node_host(N) when is_atom(N) ->
    TS = string:tokens(atom_to_list(N), "@"),
    case length(TS) > 1 of
        true -> hd(tl(TS));
        _ -> ""
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the names of the nodes in the localhost.
%% @spec
%% find_node_names() -> [string()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_node_names() -> [string()].
find_node_names() ->
    get_node_names(net_adm:names()).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the names of the nodes in a host.
%% <br/><b>Host:</b> A string with the host.
%% @spec
%% find_node_names(Host::string()) -> [string()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_node_names(Host::string()) -> [string()].
find_node_names(Host) ->
    get_node_names(net_adm:names(Host)).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Gets the names from the result given by calling net_adm:names().
%% <br/><b>Data:</b> The received value from net_adm:names().
%% @spec
%% get_node_names(Data::{'ok',[{string(),number()}]} |
%%      {'error','address' | 'einval' | 'nxdomain'}) -> [string()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_node_names(Data::{'ok',[{string(),number()}]} |
    {'error','address' | 'einval' | 'nxdomain'}) -> [string()].
get_node_names({ok, NS}) when is_list(NS) ->
    [N || {N,_} <- NS];
get_node_names(_) ->
    [].

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the the nodes in the localhost.
%% @spec
%% find_nodes() -> [atom()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_nodes() -> [atom()].
find_nodes() ->
    [list_to_atom(N ++ "@" ++ node_host()) || N <- find_node_names()].

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the the nodes in a host.
%% <br/><b>Host:</b> A string with the host.
%% @spec
%% find_nodes(Host::string()) -> [atom()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_nodes(Host::string()) -> [atom()].
find_nodes(Host) when is_list(Host) ->
    [list_to_atom(N ++ "@" ++ Host) || N <- find_node_names(Host)].

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the the nodes in a list of hosts.
%% <br/><b>Host:</b> A list of hosts.
%% @spec
%% find_nodes_from_hosts(Hosts::string()) -> [atom()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_nodes_from_hosts(Hosts::[string()]) -> [atom()].
find_nodes_from_hosts([]) ->
    find_nodes(node_host());
find_nodes_from_hosts(Hosts) when is_list(Hosts) ->
    lists:append([find_nodes(N) || N <- Hosts]).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the the nodes in a list of hosts and excludes the current node.
%% <br/><b>Host:</b> A list of hosts.
%% @spec
%% find_other_nodes_from_hosts(Hosts::string()) -> [atom()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_other_nodes_from_hosts(Hosts::[string()]) -> [atom()].
find_other_nodes_from_hosts(Hosts) when is_list(Hosts) ->
    CurrentNode = node(),
    [N || N <- find_nodes_from_hosts(Hosts), N =/= CurrentNode].

%%===========================================================================================
%% Files
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Reads all the lines inside a file.
%% <br/><b>Path:</b> The path of the file.
%% <br/><b>FailValue:</b> The value to return if the open fails.
%% @spec
%% read_lines(Path::string(),FailValue::any()) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec read_lines(Path::string(),FailValue::any()) -> any().
read_lines(Path, FailValue) when is_list(Path) ->
    case open_utf8_to_read(Path) of
        {fail, _} -> FailValue;
        {ok, IoDevice} -> read_lines_and_close(IoDevice)
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Reads all the lines inside a file.
%% <br/><b>Path:</b> The path of the file.
%% <br/><b>FailValue:</b> The value to return if the open fails.
%% <br/><b>OnLines:</b> This is called when the lines are returned.
%% @spec
%% read_lines(Path::string(),FailValue::any(),
%%      OnLines::fun(([string()]) -> any())) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec read_lines(Path::string(),FailValue::any(),
    OnLines::fun(([string()]) -> any())) -> any().
read_lines(Path, FailValue, OnLines) when is_list(Path) ->
    case open_utf8_to_read(Path) of
        {fail, _} -> FailValue;
        {ok, IoDevice} -> OnLines(read_lines_and_close(IoDevice))
    end.

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Reads all the lines inside a file and close it.
%% <br/><b>IoDevice:</b> The file handler to read the lines.
%% @spec
%% read_lines_and_close(IoDevice::file:io_device()) -> [string()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec read_lines_and_close(IoDevice::file:io_device()) -> [string()].
read_lines_and_close(IoDevice) ->
    Lines = get_lines(IoDevice),
    file:close(IoDevice),
    Lines.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Writes all the lines inside a file.
%% <br/><b>Path:</b> The path of the file.
%% <br/><b>Data:</b> The data to write in the file.
%% @spec
%% write_lines(Path::string(),Data::[any()]) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec write_lines(Path::string(),Data::[any()]) -> any().
write_lines(Path, Data) when is_list(Path), is_list(Data) ->
    case open_utf8_to_read(Path) of
        {fail, _} ->
            fail;
        {ok, IoDevice} ->
            put_lines(IoDevice, Data),
            file:close(IoDevice)
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Opens an UTF8 file.
%% <br/><b>Path:</b> The path of the file.
%% <br/><b>Mode:</b> The mode to open the file.
%% @spec
%% open_utf8(Path::string(),Mode::file:mode() | [file:mode()]) ->
%%      {'ok',file:io_device()} | {'fail',atom() | {any(),any()}}
%% @end
%%-------------------------------------------------------------------------------------------
-spec open_utf8(Path::string(),Mode::file:mode() | [file:mode()]) ->
    {'ok',file:io_device()} | {'fail',atom() | {any(),any()}}.
open_utf8(Path, Mode) when is_list(Path), is_list(Mode) ->
    try file:open(Path, [{encoding, utf8} | Mode]) of
        {ok, IoDevice} -> {ok, IoDevice};
        {error, Reason} -> {fail, Reason}
    catch
        Error:Exception -> {fail, {Error, Exception}}
    end;
open_utf8(Path, Mode) ->
    open_utf8(Path, [Mode]).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Opens an UTF8 file to read.
%% <br/><b>Path:</b> The path of the file.
%% @spec
%% open_utf8_to_read(Path::string()) -> {'ok',file:io_device()} |
%%      {'fail',atom() | {any(),any()}}
%% @end
%%-------------------------------------------------------------------------------------------
-spec open_utf8_to_read(Path::string()) -> {'ok',file:io_device()} |
    {'fail',atom() | {any(),any()}}.
open_utf8_to_read(Path) when is_list(Path) ->
    open_utf8(Path, read).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Opens an UTF8 file to write. If the file exists the file will be truncated.
%% <br/><b>Path:</b> The path of the file.
%% @spec
%% open_utf8_to_write(Path::string()) -> {'ok',file:io_device()} |
%%      {'fail',atom() | {any(),any()}}
%% @end
%%-------------------------------------------------------------------------------------------
-spec open_utf8_to_write(Path::string()) -> {'ok',file:io_device()} |
{'fail',atom() | {any(),any()}}.
open_utf8_to_write(Path) when is_list(Path) ->
    open_utf8(Path, write).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets all the lines inside an opened file.
%% <br/><b>IoDevice:</b> The file handler to read the lines.
%% @spec
%% get_lines(IoDevice::file:io_device()) -> [string()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_lines(IoDevice::file:io_device()) -> [string()].
get_lines(IoDevice) ->
    get_lines(fun () -> io:get_line(IoDevice, "") end, []).

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Gets all the lines inside an opened file.
%% <br/><b>Read:</b> The read operation.
%% <br/><b>Lines:</b> The readed lines in reverse order.
%% @spec
%% get_lines(Read::fun(() -> io:server_no_data() | binary() | string()),
%%      Lines::[string()]) -> [string()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec get_lines(Read::fun(() -> io:server_no_data() | binary() | string()),
    Lines::[string()]) -> [string()].
get_lines(Read, Lines) ->
    case Read() of
        eof ->
            lists:reverse(Lines);
        Line ->
            case lists:suffix("\n", Line) of
                true ->
                    FinalLine = lists:sublist(Line, length(Line) - 1),
                    get_lines(Read, [FinalLine | Lines]);
                _ ->
                    get_lines(Read, [Line | Lines])
            end
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Writes all the data inside a list as lines inside an opened file.
%% <br/><b>IoDevice:</b> The file handler to write the lines.
%% <br/><b>Data:</b> The data to write in the file.
%% @spec
%% put_lines(IoDevice::file:io_device(),Data::[any()]) -> 'ok'
%% @end
%%-------------------------------------------------------------------------------------------
-spec put_lines(IoDevice::file:io_device(),Data::[any()]) -> 'ok'.
put_lines(_, []) ->
    ok;
put_lines(IoDevice, [D|DS]) ->
    io:format(IoDevice, "~p~n", [D]),
    put_lines(IoDevice, DS).

%%===========================================================================================
%% Statements
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @private
%% @doc
%% Executes a condition.
%% <br/><b>Condition:</b> The condition to execute.
%% @spec
%% exec_condition(Condition::any()) -> boolean()
%% @end
%%-------------------------------------------------------------------------------------------
-spec exec_condition(Condition::any()) -> boolean().
exec_condition(Condition) when is_function(Condition) ->
    exec_condition(Condition());
exec_condition(Condition) when is_boolean(Condition) ->
    Condition;
exec_condition(_) ->
    false.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Executes an if-then-end statement. When the condition is false the function will
%% return false.
%% <br/><b>Condition:</b> The condition to check.
%% <br/><b>Code:</b> The code to execute.
%% @spec
%% if_then(Condition::any(), Code::fun(() -> any())) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec if_then(Condition::any(), Code::fun(() -> any())) -> any().
if_then(Condition, Code) when is_function(Code) ->
    case exec_condition(Condition) of
        true -> Code();
        _ -> false
    end.

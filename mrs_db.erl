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
-module(mrs_db).
-author("Gorka Suárez García").
-export([
    open/2, insert/2, insert_all/2, update/2, update_all/2, find/2, find_first/2,
    find_all/2, find_heads/2, remove/2, close/1
]).

%%===========================================================================================
%% Interface
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Opens a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>File:</b> The path of the database file.
%% @spec
%% open(DB::atom(),File::string()) -> {'ok',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec open(DB::atom(),File::string()) -> {'ok',any()}.
open(DB, File) when is_atom(DB), is_list(File) ->
    {ok, DB} = dets:open_file(DB, [{file, File},
        {access, read_write}, {type, set}, {keypos, 1}]).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Inserts a new value in a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Value:</b> The value to insert.
%% @spec
%% insert(DB::atom(),Value::[tuple()] | tuple()) -> boolean() | {'error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec insert(DB::atom(),Value::[tuple()] | tuple()) -> boolean() | {'error',any()}.
insert(DB, Value) when is_atom(DB) ->
    dets:insert_new(DB, Value).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Inserts a list of values in a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Values:</b> The values to insert.
%% @spec
%% insert_all(DB::atom(),Value::[tuple()]) -> ['ok' | {'error',any()}]
%% @end
%%-------------------------------------------------------------------------------------------
-spec insert_all(DB::atom(),Value::[tuple()]) -> ['ok' | {'error',any()}].
insert_all(DB, Values) when is_atom(DB), is_list(Values) ->
    [insert(DB, V) || V <- Values].

%%-------------------------------------------------------------------------------------------
%% @doc
%% Inserts or updates a value in a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Value:</b> The value to update.
%% @spec
%% update(DB::atom(),Value::[tuple()] | tuple()) -> 'ok' | {'error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec update(DB::atom(),Value::[tuple()] | tuple()) -> 'ok' | {'error',any()}.
update(DB, Value) when is_atom(DB) ->
    dets:insert(DB, Value).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Inserts or updates a list of values in a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Values:</b> The values to update.
%% @spec
%% update_all(DB::atom(),Value::[tuple()]) -> ['ok' | {'error',any()}]
%% @end
%%-------------------------------------------------------------------------------------------
-spec update_all(DB::atom(),Value::[tuple()]) -> ['ok' | {'error',any()}].
update_all(DB, Values) when is_atom(DB), is_list(Values) ->
    [update(DB, V) || V <- Values].

%%-------------------------------------------------------------------------------------------
%% @doc
%% Finds a value in a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Key:</b> The key of the tuple to find.
%% @spec
%% find(DB::atom(),Key::any()) -> [tuple()] | {'error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec find(DB::atom(),Key::any()) -> [tuple()] | {'error',any()}.
find(DB, Key) when is_atom(DB) ->
    dets:lookup(DB, Key).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Finds the first value in a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Key:</b> The key of the tuple to find.
%% @spec
%% find_first(DB::atom(),Key::any()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_first(DB::atom(),Key::any()) -> tuple().
find_first(DB, Key) when is_atom(DB) ->
    case dets:lookup(DB, Key) of
        [V|_] when is_tuple(V) -> V;
        _ -> {}
    end.

%%-------------------------------------------------------------------------------------------
%% @doc
%% Finds a list of keys in a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Keys:</b> The keys of the tuples to find.
%% @spec
%% find_all(DB::atom(),Keys::[any()]) -> [tuple()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_all(DB::atom(),Keys::[any()]) -> [tuple()].
find_all(DB, Keys) when is_atom(DB), is_list(Keys) ->
    lists:flatten([find(DB, K) || K <- Keys]).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Finds a list of keys in a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Keys:</b> The keys of the tuples to find.
%% @spec
%% find_heads(DB::atom(),Keys::[any()]) -> [tuple()]
%% @end
%%-------------------------------------------------------------------------------------------
-spec find_heads(DB::atom(),Keys::[any()]) -> [tuple()].
find_heads(DB, Keys) when is_atom(DB), is_list(Keys) ->
    mrs_util:get_heads([find(DB, K) || K <- Keys]).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Removes a value from a database.
%% <br/><b>DB:</b> The name of the database.
%% <br/><b>Key:</b> The key of the tuple to remove.
%% @spec
%% remove(DB::atom(),Key::any()) -> 'ok' | {'error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec remove(DB::atom(),Key::any()) -> 'ok' | {'error',any()}.
remove(DB, Key) when is_atom(DB) ->
    dets:delete(DB, Key).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Closes a database.
%% <br/><b>DB:</b> The name of the database.
%% @spec
%% close(DB::atom()) -> 'ok' | {'error',any()}
%% @end
%%-------------------------------------------------------------------------------------------
-spec close(DB::atom()) -> 'ok' | {'error',any()}.
close(DB) when is_atom(DB) ->
    dets:close(DB).

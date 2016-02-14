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
-author("Gorka Suárez García").

%%===========================================================================================
%% Constants
%%===========================================================================================

-define(ERROR, 'mrs:error').
-define(UNDEFINED, undefined).

-define(FILTER_MODE,                mrs_filter).
-define(FLATMAP_MODE,               mrs_flatmap).
-define(GROUPBY_MODE,               mrs_groupby).
-define(GROUPBYKEY_MODE,            mrs_groupbykey).
-define(MAP_MODE,                   mrs_map).
-define(REDUCE_MODE,                mrs_reduce).
-define(REDUCEBYKEY_MODE,           mrs_reducebykey).
-define(SORTBY_MODE,                mrs_sortby).
-define(SORTBYKEY_MODE,             mrs_sortbykey).
-define(ZIP_MODE,                   mrs_zip).
-define(ZIPWIDX_MODE,               mrs_zipwidx).
-define(ZIPWUID_MODE,               mrs_zipwuid).
-define(AGGREGATE_MODE,             mrs_aggregate).
-define(AGGREGATEBYKEY_MODE,        mrs_aggregatebykey).
-define(COUNT_MODE,                 mrs_count).
-define(COUNTBYKEY_MODE,            mrs_countbykey).
-define(COUNTDISTINCT_MODE,         mrs_countdistinct).
-define(COUNTDISTINCTBYKEY_MODE,    mrs_countdistinctbykey).
-define(COUNTBYVALUE_MODE,          mrs_countbyvalue).
-define(FOLDR_MODE,                 mrs_foldr).
-define(FOLDRBYKEY_MODE,            mrs_foldrbykey).
-define(FOLDL_MODE,                 mrs_foldl).
-define(FOLDLBYKEY_MODE,            mrs_foldlbykey).
-define(CARTESIAN_MODE,             mrs_cartesian).
-define(DISTINCT_MODE,              mrs_distinct).
-define(INTERSECTION_MODE,          mrs_intersection).
-define(SUBTRACT_MODE,              mrs_subtract).
-define(UNION_MODE,                 mrs_union).
-define(TAKE_MODE,                  mrs_take).
-define(ISEMPTY_MODE,               mrs_isempty).

%%===========================================================================================
%% Records
%%===========================================================================================

-record(mrs_query, {ref = make_ref(), data, steps}).
-record(mrs_step, {mode, params}).

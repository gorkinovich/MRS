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

-define(JOB_TUPLE_KEY,      1).
-define(JOB_TUPLE_OWNER,    2).
-define(JOB_TUPLE_DATA,     3).
-define(JOB_TUPLE_CSTEPS,   4).
-define(JOB_TUPLE_NSTEPS,   5).
-define(JOB_TUPLE_CHUNKSZ,  6).
-define(JOB_TUPLE_TCHUNKS,  7).
-define(JOB_TUPLE_SCHUNKS,  8).
-define(JOB_TUPLE_PCHUNKS,  9).
-define(JOB_TUPLE_RCHUNKS, 10).
-define(JOB_TUPLE_SIZE,    10).

-define(MAX_CHUNK_ELEMS, 32).

-define(JOB_FINISHED, finished).
-define(PCHUNK_NOWORKER, null).

-record(mrs_task, {ref = make_ref(), worker, qref, step, index, chunk}).

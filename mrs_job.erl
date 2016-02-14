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
-module(mrs_job).
-author("Gorka Suárez García").
-include("mrs_api.hrl").
-include("mrs_job.hrl").
-export([
    new/2, key/1, owner/1, data/1, current_step/1, next_steps/1, chunk_size/1,
    total_chunks/1, sended_chunks/1, pending_chunks/1, received_chunks/1, key/2,
    owner/2, data/2, current_step/2, next_steps/2, chunk_size/2, total_chunks/2,
    sended_chunks/2, pending_chunks/2, received_chunks/2
]).

%%===========================================================================================
%% Constructors
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Makes a new job tuple from an owner PID and a query.
%% <br/><b>Owner:</b> The owner of the job.
%% <br/><b>Query:</b> The query of the job.
%% @spec
%% new(Owner::pid(),Query::#mrs_query{}) ->
%%      {atom(),pid(),any(),any(),queue:queue(#mrs_step{}),0,0,0,[],[]}
%% @end
%%-------------------------------------------------------------------------------------------
-spec new(Owner::pid(),Query::#mrs_query{}) ->
    {atom(),pid(),any(),any(),queue:queue(#mrs_step{}),0,0,0,[],[]}.
new(Owner, Query) when is_pid(Owner), is_record(Query, mrs_query) ->
    % Taking the first step in the query:
    {CurrentStep, NextSteps} = mrs_util:queue_out(Query#mrs_query.steps, ?JOB_FINISHED),
    % Making a key atom with the reference:
    Key = mrs_util:ref_to_atom(Query#mrs_query.ref),
    % Making the final job tuple value:
    {
        Key,                    % key
        Owner,                  % owner
        Query#mrs_query.data,   % data
        CurrentStep,            % current_step
        NextSteps,              % next_steps
        0,                      % chunk_size
        0,                      % total_chunks
        0,                      % sended_chunks
        [],                     % pending_chunks
        []                      % received_chunks
    }.

%%===========================================================================================
%% Getters
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the key of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% key(T::tuple()) -> atom()
%% @end
%%-------------------------------------------------------------------------------------------
-spec key(T::tuple()) -> atom().
key(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_KEY, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the owner of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% owner(T::tuple()) -> pid()
%% @end
%%-------------------------------------------------------------------------------------------
-spec owner(T::tuple()) -> pid().
owner(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_OWNER, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the data of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% data(T::tuple()) -> any()
%% @end
%%-------------------------------------------------------------------------------------------
-spec data(T::tuple()) -> any().
data(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_DATA, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the current step of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% current_step(T::tuple()) -> atom() | #mrs_step{}
%% @end
%%-------------------------------------------------------------------------------------------
-spec current_step(T::tuple()) -> atom() | #mrs_step{}.
current_step(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_CSTEPS, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the next steps of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% next_steps(T::tuple()) -> queue:queue()
%% @end
%%-------------------------------------------------------------------------------------------
-spec next_steps(T::tuple()) -> queue:queue().
next_steps(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_NSTEPS, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the chunk size of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% chunk_size(T::tuple()) -> number()
%% @end
%%-------------------------------------------------------------------------------------------
-spec chunk_size(T::tuple()) -> number().
chunk_size(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_CHUNKSZ, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the total chunks of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% total_chunks(T::tuple()) -> number()
%% @end
%%-------------------------------------------------------------------------------------------
-spec total_chunks(T::tuple()) -> number().
total_chunks(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_TCHUNKS, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the sended chunks of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% sended_chunks(T::tuple()) -> number()
%% @end
%%-------------------------------------------------------------------------------------------
-spec sended_chunks(T::tuple()) -> number().
sended_chunks(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_SCHUNKS, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the pending chunks of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% pending_chunks(T::tuple()) -> maybe_improper_list()
%% @end
%%-------------------------------------------------------------------------------------------
-spec pending_chunks(T::tuple()) -> maybe_improper_list().
pending_chunks(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_PCHUNKS, T).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Gets the received chunks of the job.
%% <br/><b>T:</b> The tuple with the job.
%% @spec
%% received_chunks(T::tuple()) -> maybe_improper_list()
%% @end
%%-------------------------------------------------------------------------------------------
-spec received_chunks(T::tuple()) -> maybe_improper_list().
received_chunks(T) when size(T) =:= ?JOB_TUPLE_SIZE ->
    element(?JOB_TUPLE_RCHUNKS, T).

%%===========================================================================================
%% Setters
%%===========================================================================================

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the key of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% key(T::tuple(),V::atom()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec key(T::tuple(),V::atom()) -> tuple().
key(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_KEY, T, V, fun is_atom/1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the owner of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% owner(T::tuple(),V::pid()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec owner(T::tuple(),V::pid()) -> tuple().
owner(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_OWNER, T, V, fun is_pid/1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the data of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% data(T::tuple(),V::any()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec data(T::tuple(),V::any()) -> tuple().
data(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    setelement(?JOB_TUPLE_DATA, T, V).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the current step of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% current_step(T::tuple(),V::atom() | #mrs_step{}) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec current_step(T::tuple(),V::atom() | #mrs_step{}) -> tuple().
current_step(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_CSTEPS, T, V,
        fun(X) -> is_record(X, mrs_step) orelse X =:= ?JOB_FINISHED end).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the next steps of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% next_steps(T::tuple(),V::queue:queue()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec next_steps(T::tuple(),V::queue:queue()) -> tuple().
next_steps(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_NSTEPS, T, V, fun queue:is_queue/1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the chunk size of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% chunk_size(T::tuple(),V::number()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec chunk_size(T::tuple(),V::number()) -> tuple().
chunk_size(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_CHUNKSZ, T, V, fun mrs_util:is_natural/1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the total chunks of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% total_chunks(T::tuple(),V::number()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec total_chunks(T::tuple(),V::number()) -> tuple().
total_chunks(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_TCHUNKS, T, V, fun mrs_util:is_natural/1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the sended chunks of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% sended_chunks(T::tuple(),V::number()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec sended_chunks(T::tuple(),V::number()) -> tuple().
sended_chunks(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_SCHUNKS, T, V, fun mrs_util:is_natural/1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the pending chunks of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% pending_chunks(T::tuple(),V::maybe_improper_list()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec pending_chunks(T::tuple(),V::maybe_improper_list()) -> tuple().
pending_chunks(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_PCHUNKS, T, V, fun is_list/1).

%%-------------------------------------------------------------------------------------------
%% @doc
%% Sets the received chunks of the job.
%% <br/><b>T:</b> The tuple with the job.
%% <br/><b>V:</b> The value to set.
%% @spec
%% received_chunks(T::tuple(),V::maybe_improper_list()) -> tuple()
%% @end
%%-------------------------------------------------------------------------------------------
-spec received_chunks(T::tuple(),V::maybe_improper_list()) -> tuple().
received_chunks(T, V) when size(T) =:= ?JOB_TUPLE_SIZE ->
    mrs_util:setelement(?JOB_TUPLE_RCHUNKS, T, V, fun is_list/1).

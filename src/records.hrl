%%%-------------------------------------------------------------------
%%% @author olegb
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Feb 2021 09:48
%%%-------------------------------------------------------------------
-author("olegb").

-record(job, {id, name, department}).
-record(employee, {id, name, age, location, job = #job{}}).
-record(erlang_sequence, {name, seq}).




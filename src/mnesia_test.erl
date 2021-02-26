%%%-------------------------------------------------------------------
%%% @author oleg
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Feb 2021 10:25
%%%-------------------------------------------------------------------
-module(mnesia_test).
-author("olegb").
-include("records.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([init/0, insert_job/2, insert_employee/4,
  read/1, read/2, delete/2, update/3,
  read_all/0, update_job/3, read_all_jobs/0]).


init() ->
  mnesia:create_schema([node()]),
  mnesia:start(),
  {atomic, ok} = mnesia:create_table(erlang_sequence, [{record_name, erlang_sequence}, {access_mode, read_write}, {disc_copies, [node()]}, {attributes, record_info(fields, erlang_sequence)}]),
  {atomic, ok} = mnesia:create_table(job, [{record_name, job}, {access_mode, read_write}, {disc_copies, [node()]}, {attributes, record_info(fields, job)}]),
  {atomic, ok} = mnesia:create_table(employee, [{record_name, employee}, {access_mode, read_write}, {disc_copies, [node()]}, {attributes, record_info(fields, employee)}]).

insert_job(Name, Department) ->
  InsertFun = fun() ->
    JobId = mnesia:dirty_update_counter(erlang_sequence, job, 1),
    Record = #job{
      id = JobId,
      name = Name,
      department = Department
    },
    mnesia:write(job, Record, write) end,
  mnesia:transaction(InsertFun),
  mnesia:transaction(fun() ->
    mnesia:match_object(job, #job{_ = '_'}, write) end).

insert_employee(Name, Age, Location, Job_Name) ->
  EmployeeRecord = fun() ->
    JobRecord = get_job_record(Job_Name),
    EmployeeId = mnesia:dirty_update_counter(erlang_sequence, employee, 1),
    Record = #employee{
      id = EmployeeId,
      name = Name,
      age = Age,
      location = Location,
      job = JobRecord
    },
    mnesia:write(employee, Record, write)
                   end,
  mnesia:transaction(EmployeeRecord),
  mnesia:transaction(fun() ->
    mnesia:match_object(employee, #employee{_ = '_'}, write) end).

%% Query element by any key/s where L = [{key1, value1}, {key2, value2}, ...]
read(L) when is_list(L) ->
  MatchHead = #employee{id = '$1', name = '$2', age = '$3', location = '$4', job = #job{name = '$5', department = '$6', _ = '_'}},
  Guard = get_guard_list(L),
  Result = '$_',
  mnesia:activity(transaction, fun() -> mnesia:select(employee, [{MatchHead, Guard, [Result]}], write) end).

%% Query with one key-value
read(Key, Value) ->
  case Key of
    id -> mnesia:activity(transaction, fun() -> mnesia:read(employee, Value, write) end);
    name ->
      {_, L} = mnesia:transaction(fun() -> mnesia:match_object(employee, #employee{name = Value, _ = '_'}, write) end),
      L;
    age ->
      MatchHead = #employee{age = '$1', _ = '_'},
      Guard = [{'>=', '$1', Value}],
      Result = '$_',
      mnesia:activity(transaction, fun() -> mnesia:select(employee, [{MatchHead, Guard, [Result]}], write) end);
    location ->
      {_, L} = mnesia:transaction(fun() ->
        mnesia:match_object(employee, #employee{location = Value, _ = '_'}, write) end), L;
    job ->
      {_, L} = mnesia:transaction(fun() ->
        mnesia:match_object(employee, #employee{job = #job{name = Value, _ = '_'}, _ = '_'}, write) end), L;
    department ->
      {_, L} = mnesia:transaction(fun() ->
        mnesia:match_object(employee, #employee{job = #job{department = Value, _ = '_'}, _ = '_'}, write) end), L
  end.

delete(Key, Value) ->
  L = read(Key, Value),
  case Key of
    id -> mnesia:transaction(fun() -> mnesia:delete(employee, Value, write) end);
    age ->
      DeleteFun = fun(X) -> mnesia:delete_object(X) end,
      mnesia:transaction(fun() -> lists:foreach(DeleteFun, L) end);

    name -> get_delete_action(L);
    location -> get_delete_action(L);
    job -> get_delete_action(L);
    department -> get_delete_action(L);
    _ -> field_not_found
  end.

update(Id, Key, NewValue) when is_integer(Id) ->
  UpdateFun = fun() ->
    Record = read(id, Id),
    over_write(Record, Key, NewValue) end,
  mnesia:transaction(UpdateFun).

update_job(Id, Key, NewValue) when is_integer(Id) ->
  UpdateJobFun = fun() ->
    OldJob = mnesia:activity(transaction, fun() -> mnesia:read(job, Id, write) end),
    over_write_job(OldJob, Key, NewValue) end,
  mnesia:activity(transaction, UpdateJobFun).

read_all() ->
  mnesia:transaction(fun() ->
    mnesia:match_object(employee, #employee{_ = '_'}, write) end).

read_all_jobs() ->
  mnesia:transaction(fun() ->
    mnesia:match_object(job, #job{_ = '_'}, write) end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_job_record(Job_Name) ->
  Q = qlc:q([J || J <- mnesia:table(job), J#job.name =:= Job_Name]),
  Ev = qlc:e(Q),
  get_record(Ev).

get_record(Ev) ->
  if
    Ev =:= [] -> job_do_not_exist;
    true -> hd(Ev)
  end.

get_guard_list(L) -> get_all_keys(L, []).

get_all_keys([H | T], Acc) ->
  {K, V} = H,
  KP = get_key_place(K),
  get_all_keys(T, [{'=:=', KP, V} | Acc]);
get_all_keys([], Acc) -> Acc.


get_key_place(K) ->
  case K of
    id -> '$1';
    name -> '$2';
    age -> '$3';
    location -> '$4';
    job -> '$5';
    department -> '$6'
  end.

get_delete_action(L) ->
  case L of
    [] -> ok;
    _ -> mnesia:transaction(fun() -> mnesia:delete_object(employee, hd(L), write) end)
  end.

over_write([E | _], Key, NewValue) ->
  case Key of
    name -> New = E#employee{name = NewValue},
      mnesia:write(New);
    age -> New = E#employee{age = NewValue},
      mnesia:write(New);
    location ->
      New = E#employee{location = NewValue},
      mnesia:write(New);
    job -> New = E#employee{job = get_job_record(NewValue)},
      mnesia:write(New)
  end;
over_write([], _, _) -> ok.

%% The same function but for multiple elements
%%over_write([E | Tail], Key, NewValue) ->
%%  case Key of
%%    name -> New = E#employee{name = NewValue},
%%      mnesia:write(New),
%%      1 + over_write(Tail, Key, NewValue);
%%    age -> New = E#employee{age = NewValue},
%%      mnesia:write(New),
%%      1 + over_write(Tail, Key, NewValue);
%%    location ->
%%      New = E#employee{location = NewValue},
%%      mnesia:write(New),
%%      1 + over_write(Tail, Key, NewValue);
%%    job -> New = E#employee{job = get_job_record(NewValue)},
%%      mnesia:write(New),
%%      1 + over_write(Tail, Key, NewValue)
%%  end;

over_write_job([J | _], Key, NewValue) ->
  case Key of
    name -> New = J#job{name = NewValue},
      mnesia:write(New);
    department -> New = J#job{department = NewValue},
      mnesia:write(New)
  end;
over_write_job([], _, _) -> ok.
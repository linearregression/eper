%%%-------------------------------------------------------------------
%%% File    : watchdog.erl
%%% Author  : Mats Cronqvist <etxmacr@mwux005>
%%% Description :
%%%
%%% Created : 11 Mar 2005 by Mats Cronqvist <etxmacr@mwux005>
%%%-------------------------------------------------------------------
-module(watchdog).

% API
-export(
   [config/2
    ,start/0,stop/0,state/0
    ,add_send_subscriber/4,add_log_subscriber/1,add_proc_subscriber/1
    ,delete_subscriber/1,reset_subscriber/1
    ,delete_subscribers/0,reset_subscribers/0
    ,add_trigger/2,delete_trigger/1
    ,message/1]).

% gen_serv callbacks
-export(
   [handle_info/2
    ,init/1
    ,rec_info/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("kernel/include/file.hrl").
-include("log.hrl").

-record(ld,
        {timeout_restart=5000         %turn off sys_mon this long @ max_jailed
         ,max_jailed=20               %turn off sys_mon when jail is this full
         ,timeout_release=100         %keep pid/msg_type in jail this long
         ,cache_connections=true      %keep udp/tcp sockets open

         ,jailed=[]                      %jailed pids
         ,subscribers=[]                 %where to send our reports
         ,triggers=default_triggers()    %{atom(Tag), fun/1->no|fun/1}
         ,prfState                       %prfTarg:subscribe return val
         ,userData                       %last user data
         ,prfData                        %last prf data
         ,monData                        %last system_monitor data
        }).

rec_info(ld) -> record_info(fields,ld);
rec_info(_)  -> [].

show_these_fields() ->
  [timeout_restart,max_jailed,timeout_release,cache_connections,
   jailed,subscribers,triggers].

upgrade({ld,TR,MJ,TS,Js,Ss,Ts,PS,UD,PD,MD}) ->
  #ld{timeout_restart=TR
     ,max_jailed=MJ
     ,timeout_release=TS
     ,cache_connections=false
     ,jailed=Js
     ,subscribers=Ss
     ,triggers=Ts
     ,prfState=PS
     ,userData=UD
     ,prfData=PD
     ,monData=MD
     }.

%% constants

default_triggers() ->
  [ {[sysMon,long_gc],500}             %gc time [ms]
   ,{[sysMon,long_schedule],500}       %scheduled time [ms]
   ,{[sysMon,large_heap],1024*1024}    %heap size [words]
   ,{[sysMon,busy_port],true}
   ,{[sysMon,busy_dist_port],true}
   ,{user,true}
   ,{ticker,true}
   ,{[prfSys,user],fun(X)->true=(0.95<X) end}
   ,{[prfSys,kernel],fun(X)->true=(0.5<X) end}
   ,{[prfSys,iowait],fun(X)->true=(0.3<X) end}
  ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% api
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
  case gen_serv:start(?MODULE) of
    {ok,Pid} ->  Pid;
    {error,{already_started,Pid}} -> Pid
  end.

stop() ->
  gen_serv:stop(?MODULE).

state() ->
  handle_state().

config(prfPrc,{max_procs,MaxProcs}) ->
  prfTarg:config({prfPrc,{max_procs,MaxProcs}});
config(Tag,Val) ->
  send_to_wd({cfg,Tag,Val}),
  state().

delete_trigger(Key) ->
  send_to_wd({delete_trigger,Key}).

add_trigger(Key,Val) ->
  send_to_wd({add_trigger,Key,Val}).

add_proc_subscriber(Pid) when is_pid(Pid) ->
  case is_process_alive(Pid) of
    true -> send_to_wd({add_subscriber,{{pid,Pid},''}});
    false-> {error,no_such_pid}
  end;
add_proc_subscriber(Reg) when is_atom(Reg) ->
  send_to_wd({add_subscriber,{{pid,{Reg,node()}},''}});
add_proc_subscriber({Reg,Node}) when is_atom(Reg),is_atom(Node) ->
  send_to_wd({add_subscriber,{{pid,{Reg,Node}},''}}).

%% E.g:  watchdog:add_send_subscriber(tcp,"localhost",56669,"I'm a Cookie").
add_send_subscriber(Proto,Host,Port,PassPhrase) ->
  case inet:gethostbyname(Host) of
    {ok,_}    -> send_to_wd({add_subscriber,{{Proto,{Host,Port}},PassPhrase}});
    {error,R} -> {error,R}
  end.

add_log_subscriber({trc,FN}) ->
  send_to_wd({add_subscriber,{{log,trc},FN}});
add_log_subscriber({text,FN}) ->
  send_to_wd({add_subscriber,{{text,trc},FN}});
add_log_subscriber(screen) ->
  send_to_wd({add_subscriber,{{log,screen},''}}).

delete_subscribers() ->
  send_to_wd(delete_subscribers).

delete_subscriber(Key) ->
  send_to_wd({delete_subscriber,Key}).

reset_subscribers() ->
  send_to_wd(reset_subscribers).

reset_subscriber(Key) ->
  send_to_wd({reset_subscriber,Key}).

message(Term) ->
  send_to_wd({user,Term}).

send_to_wd(Term) ->
  try
    ?MODULE ! Term,
    ok
  catch
    error:badarg   -> {error,watchdog_not_started};
    _:R            -> {error,R}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
  LD = #ld{prfState=prfTarg:subscribe(node(),self(),[prfSys,prfPrc])},
  start_monitor(LD#ld.triggers),
  LD.

% upgrade
handle_info(Msg,OLD) when not is_record(OLD,ld) ->
  handle_info(Msg,upgrade(OLD));

% admin configs
handle_info({cfg,timeout_restart,TR},LD) when is_integer(TR) ->
  LD#ld{timeout_restart=TR};
handle_info({cfg,max_jailed,MJ},LD)      when is_integer(MJ) ->
  LD#ld{max_jailed=MJ};
handle_info({cfg,timeout_release,TR},LD) when is_integer(TR)->
  LD#ld{timeout_release=TR};
handle_info({cfg,cache_connections,TR},LD) when is_boolean(TR)->
  LD#ld{cache_connections=TR};

% admin triggers
handle_info({delete_trigger,Key},LD) ->
  LD#ld{triggers=delete_trigger(LD#ld.triggers,Key)};
handle_info({add_trigger,Key,Val},LD) ->
  LD#ld{triggers=add_trigger(LD#ld.triggers,Key,Val)};

% admin subscribers
handle_info(reset_subscribers,LD) ->
  LD#ld{subscribers=reset_subscribers(LD)};
handle_info({reset_subscriber,Key},LD) ->
  LD#ld{subscribers=reset_subscriber(Key,LD)};
handle_info(delete_subscribers,LD) ->
  LD#ld{subscribers=delete_subscribers(LD)};
handle_info({delete_subscriber,Key},LD) ->
  LD#ld{subscribers=delete_subscriber(Key,LD)};
handle_info({add_subscriber,{Key,Val}},LD) ->
  LD#ld{subscribers=add_subscriber(Key,Val,LD)};

% events
handle_info(trigger,LD) -> % fake trigger for debugging
  send_report(LD,test),
  LD;
handle_info({user,Data},LD) -> % data from user
  NLD = LD#ld{userData=Data},
  try do_user(check_jailed(NLD,userData))
  catch _ -> NLD
  end;
handle_info({{data,_},Data},LD) -> % data from prfTarg
  erlang:garbage_collect(self()),
  NLD = LD#ld{prfData=Data},
  try do_triggers(check_jailed(NLD,prfData))
  catch _ -> NLD
  end;
handle_info({monitor,Pid,Tag,Data},LD) -> % data from system_monitor
  NLD = LD#ld{monData=[{tag,Tag},{pid,Pid},{data,Data}]},
  try do_mon(check_jailed(NLD,Pid))
  catch _ -> NLD
  end;

% timeouts
handle_info({timeout,_,restart},LD) -> % restarting after timeout
  start_monitor(LD#ld.triggers),
  LD#ld{jailed=[]};
handle_info({timeout,_,{release,Pid}},LD) -> % release a pid from jail
  LD#ld{jailed = LD#ld.jailed--[Pid]};

% "this shouldn't happen"(TM)
handle_info(X,LD) ->
  ?log({unexpected_msg,X}),
  LD.

handle_state() ->
  handle_state(show_these_fields()).

handle_state(Ks) ->
  try
    State = gen_serv:get_state(?MODULE),
    [I || {Key,_} = I <- State,lists:member(Key,Ks)]
  catch
    _:R -> R
  end.

start_monitor(Triggers) ->
  erlang:system_monitor(self(),sysmons(Triggers)).

sysmons(Triggers) ->
  IsImplemented =
    fun(C) ->
        try erlang:system_monitor(self(),[C]),true
        catch _:_ -> false
        after erlang:system_monitor(undefined)
        end
    end,
  SysMons = fun({[sysMon,Tag],true},Acc)-> [Tag|Acc];
               ({[sysMon,Tag],Val},Acc) -> [{Tag,Val}|Acc];
               (_,Acc)                  -> Acc
            end,
  [SM || SM <- lists:foldl(SysMons,[],Triggers),IsImplemented(SM)].

stop_monitor() ->
  erlang:system_monitor(undefined).

%% trigger() :: {ID,Val}
%% Val :: number() | function()
%% ID :: list(Tag)
%% Tag :: atom() - tags into the data structure. i.e. [prfSys,iowait]
add_trigger(Triggers,ID,Val) ->
  maybe_restart(ID,[{ID,Val}|clean_triggers(Triggers,[ID])]).

delete_trigger(Triggers,ID) ->
  maybe_restart(ID,clean_triggers(Triggers,[ID])).

maybe_restart(Trig,Triggers) ->
  case Trig of
    [sysMon|_] -> stop_monitor(),start_monitor(Triggers);
    _ -> ok
  end,
  Triggers.

clean_triggers(Triggers,IDs) ->
  lists:filter(fun({ID,_})-> not lists:member(ID,IDs) end,Triggers).

check_jailed(LD,_) when LD#ld.max_jailed < length(LD#ld.jailed) ->
  % we take a timeout when enough pids are jailed. conservative is good.
  erlang:start_timer(LD#ld.timeout_restart,self(),restart),
  stop_monitor(),
  flush(),
  throw(taking_timeout);
check_jailed(LD = #ld{timeout_release=0},_) ->
  LD;
check_jailed(LD,What) ->
  case lists:member(What,LD#ld.jailed) of
    true ->
      throw(is_jailed);
    false->
      erlang:start_timer(LD#ld.timeout_release,self(),{release,What}),
      LD#ld{jailed=[What|LD#ld.jailed]}
  end.

do_mon(LD) ->
  send_report(LD,sysMon),
  LD.

do_user(LD) ->
  send_report(LD,user),
  LD.

do_triggers(LD) ->
  {Triggered,NewTriggers} = check_triggers(LD#ld.triggers,LD#ld.prfData),
  [send_report(LD,Trig) || Trig <- Triggered],
  LD#ld{triggers=NewTriggers}.

check_triggers(Triggers,Data) ->
  case check_triggers(Triggers,Data,[]) of
    [] -> {[],Triggers};
    Trigd -> IDs = [ID || {ID,_} <- Trigd],
             {IDs,Trigd++clean_triggers(Triggers,IDs)}
  end.

check_triggers([],_,O) -> O;
check_triggers([T|Ts],Data,O) ->
  check_triggers(Ts,Data,try [check_trigger(T,Data)|O] catch _:_-> O end).

check_trigger({ticker,true},_Data) ->
  {ticker,true};
check_trigger({ID,T},Data) when is_function(T) ->
  case T(get_measurement(ID,Data)) of
    true -> {ID,T};
    NewT when is_function(NewT)  -> {ID,NewT}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% admin subscribers
%%   reset
reset_subscribers(LD) ->
  DoResetSubscriber2 = fun(KF,Acc) -> do_reset_subscriber(KF,Acc,LD) end,
  lists:foldl(DoResetSubscriber2,[],LD#ld.subscribers).

reset_subscriber(Key,LD) ->
  case lists:keytake(Key,1,LD#ld.subscribers) of
    false          -> LD#ld.subscribers;
    {value,KF,Sbs} -> do_reset_subscriber(KF,Sbs,LD)
  end.

do_reset_subscriber({Key,F},Acc,LD) ->
  try [{Key,F(reset,LD)}|Acc]
  catch _:_ -> Acc
  end.

%%   delete
delete_subscribers(#ld{subscribers=Subs}) ->
  lists:foreach(fun do_delete_subscriber/1,Subs),
  [].

delete_subscriber(Key,#ld{subscribers=Subs}) ->
  case lists:keytake(Key,1,Subs) of
    {value,KF,Sbs} -> do_delete_subscriber(KF),Sbs;
    false          -> Subs
  end.

do_delete_subscriber({_,F}) ->
  catch F(stop,'').

%%   add
add_subscriber(Key,Val,LD = #ld{subscribers=Subs}) ->
  try Sub = mk_subscriber(Key,Val,LD),
      case lists:keysearch(Key,1,Subs) of
        false -> [{Key,Sub}|Subs];
        _     -> lists:keyreplace(Key,1,Subs,{Key,Sub})
      end
  catch _:R ->
      ?log([{subscriber_not_added,R},{Key,Val}]),
      Subs
    end.

%% make report and send to all subscribers
send_report(LD,Trigger) ->
  Report = make_report(Trigger,LD),
  [Sub(send,Report) || {_,Sub} <- LD#ld.subscribers].

make_report(user,LD) ->
  reporter(user,LD#ld.userData);
make_report(sysMon,LD) ->
  reporter(sysMon,expand_ps(LD#ld.monData));
make_report(Trigger,LD) ->
  reporter(Trigger,LD#ld.prfData).

reporter(Trigger,TriggerData) ->
  {?MODULE,node(),now(),Trigger,TriggerData}.

expand_ps([])                         -> [];
expand_ps([{T,P}|Es]) when is_pid(P)  -> pii({T,P})++expand_ps(Es);
expand_ps([{T,P}|Es]) when is_port(P) -> poi({T,P})++expand_ps(Es);
expand_ps([E|Es])                     -> [E|expand_ps(Es)].

pii({T,Pid}) ->
  [{T,Pid},{pid_info,prfPrc:pid_info(Pid)}].

poi({T,Port}) ->
  {Name,Stats} = prfNet:port_info(Port),
  [{T,Port},{port_name,Name},{port_info,Stats}].

flush() ->
  receive {monitor,_,_,_} -> flush()
  after 0 -> ok
  end.

get_measurement([T|Tags],Data) -> get_measurement(Tags,lks(T,Data));
get_measurement([],Data) -> Data.

lks(Tag,List) ->
  try {value,{Tag,Val}} = lists:keysearch(Tag,1,List),Val
  catch _:_ -> throw({no_such_key,Tag})
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% subscribers.
%% a subscriber is a fun/2.
%% Subscriber(send,Term) -> void(): sends term somewhere.
%% Subscriber(reset,LD) -> Subscriber/2: makes new Subscriber
%% Subscriber(close,_) -> void(): closes Subscriber, e.g. by doing file:close/1

mk_subscriber({pid,To},_,_)              -> mk_send(To);
mk_subscriber({Proto,{Host,Port}},Pwd,LD)-> mk_send(Proto,Host,Port,Pwd,LD);
mk_subscriber({log,trc},FN,_)            -> mk_log(trc,FN);
mk_subscriber({log,text},FN,_)           -> mk_log(text,FN);
mk_subscriber({log,screen},_,_)          -> mk_log(text,screen).

%% send subscribers
mk_send(Where) ->
  fun(send,Chunk) -> try Where ! Chunk catch _:_ -> ok end;
     (reset,_)    -> mk_send(Where);
     (close,_)    -> ok
  end.

mk_send(Proto,Host,Port,Cookie,LD) ->
  mk_send({Proto,LD#ld.cache_connections},Host,Port,Cookie).

mk_send({udp,true},Host,Port,Cookie) ->
  try
    {ok,Hostent} = inet:gethostbyname(Host),
    Addr = hd(Hostent#hostent.h_addr_list),
    {ok,Sck} = gen_udp:open(0,[binary]),
    fun(close,_) ->
        gen_udp:close(Sck);
       (reset,NLD) ->
        mk_send(udp,Host,Port,Cookie,NLD);
       (send,Chunk) ->
        try gen_udp:send(Sck,Addr,Port,mk_payload(Chunk,Cookie))
        catch _:_ -> ok
        end
    end
  catch _:R ->
    ?log({didnt_make_subscriber,udp,R}),
    fun(close,_)   -> ok;
       (reset,NLD) -> mk_send(udp,Host,Port,Cookie,NLD);
       (send,_)    -> ok
    end
  end;
mk_send({udp,false},Host,Port,Cookie) ->
  fun(close,_) ->
      ok;
     (reset,NLD) ->
      mk_send(udp,Host,Port,Cookie,NLD);
     (send,Chunk) ->
      try
        {ok,Sck} = gen_udp:open(0,[binary]),
        gen_udp:send(Sck,Host,Port,mk_payload(Chunk,Cookie)),
        gen_udp:close(Sck)
      catch _:_ -> ok
      end
  end;
mk_send({tcp,false},Host,Port,Cookie) ->
  fun(close,_) ->
      ok;
     (reset,NLD) ->
      mk_send(tcp,Host,Port,Cookie,NLD);
     (send,Chunk) ->
      try
        ConnOpts = [{send_timeout,100},{active,false},binary],
        ConnTimeout = 100,
        {ok,Sck} = gen_tcp:connect(Host,Port,ConnOpts,ConnTimeout),
        try gen_tcp:send(Sck,mk_payload(Chunk,Cookie))
        after gen_tcp:close(Sck)
        end
      catch _:_ -> ok
      end
  end;
mk_send({tcp,true},Host,Port,Cookie) ->
  try
    ConnOpts = [{send_timeout,100},{active,false},binary],
    ConnTimeout = 100,
    {ok,Sck} = gen_tcp:connect(Host,Port,ConnOpts,ConnTimeout),
    fun(close,_) ->
        gen_tcp:close(Sck);
       (reset,NLD) ->
        gen_tcp:close(Sck),
        mk_send(tcp,Host,Port,Cookie,NLD);
       (send,Chunk)->
        try gen_tcp:send(Sck,mk_payload(Chunk,Cookie))
        catch _:_ -> ok
        end
    end
  catch _:R ->
    ?log({didnt_make_subscriber,tcp,R}),
    fun(close,_)   -> ok;
       (reset,NLD) -> mk_send(tcp,Host,Port,Cookie,NLD);
       (send,_)    -> ok
    end
  end.

mk_payload(Chunk,Cookie) ->
  BC = term_to_binary(Chunk,[{compressed,3}]),
  Payload = prf_crypto:encrypt(Cookie,BC),
  PaySize = byte_size(Payload),
  <<PaySize:32,Payload/binary>>.

%% log subscribers
mk_log(Type,FN) ->
  FD = open_log_file(FN),
  fun(close,_) ->
      close_log_file(FD);
     (reset,_) ->
      close_log_file(FD),
      mk_log(text,FN);
     (send,Term)->
      send_log_term(Type,FD,Term)
  end.

send_log_term(text,FD,Term) ->
  print_term(FD,expand_recs(Term));
send_log_term(trc,FD,Term) ->
  S = byte_size(Term),
  file:write(FD,<<0,S:32/integer,Term/binary>>).

open_log_file(FN) ->
  case FN of
    screen -> group_leader();
    _      -> open_file(FN)
  end.

close_log_file(FD) when is_record(FD,file_descriptor) -> file:close(FD);
close_log_file(_) -> ok.

open_file(FN) ->
  ok = filelib:ensure_dir(FN),
  {ok,FD} = file:open(FN,[write,raw,binary]),
  FD.

print_term(FD,Term) ->
  case node(FD) == node() of
    true -> ?log(Term);
    false-> io:fwrite(FD," ~p~n",[Term])
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
expand_recs(List) when is_list(List) -> [expand_recs(L)||L<-List];
expand_recs(Tup) when is_tuple(Tup) ->
  case tuple_size(Tup) of
    L when L < 1 -> Tup;
    L ->
      Fields = rec_info(element(1,Tup)),
      case L == length(Fields)+1 of
        false-> list_to_tuple(expand_recs(tuple_to_list(Tup)));
        true -> expand_recs(lists:zip(Fields,tl(tuple_to_list(Tup))))
      end
  end;
expand_recs(Term) -> Term.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% eunit
%%lists:member(shell,[element(1,T)||T<-erlang:get_stacktrace()]).

start_stop_test() ->
  watchdog:start(),
  receive after 1000 -> ok end,
  PR = mk_receiver(udp),
  watchdog:add_send_subscriber(udp,"localhost",16#dada,"PWD"),
  watchdog:state(),
  watchdog:message(truism),
  ?assert(case do_receive(PR) of
              {watchdog,'nonode@nohost',_,user,truism} -> true;
              _ -> false
          end),
  watchdog:stop().

subscriber_send_proc_test() ->
  SF = mk_subscriber({pid,self()},'',''),
  SF(send,woohoo),
  ?assert(receive woohoo -> true after 0 -> false end),
  NSF = SF(reset,''),
  NSF(close,'').

subscriber_send_udp_nocache_test() ->
  PR = mk_receiver(udp),
  SF = mk_sender(udp,#ld{cache_connections=false}),
  ?assert(send_receive(PR,SF)),
  NSF = SF(reset,#ld{cache_connections=false}),
  NSF(close,'').

subscriber_send_udp_cache_test() ->
  PR = mk_receiver(udp),
  SF = mk_sender(udp,#ld{cache_connections=true}),
  ?assert(send_receive(PR,SF)),
  NSF = SF(reset,#ld{cache_connections=true}),
  NSF(close,'').

subscriber_send_tcp_nocache_test() ->
  PR = mk_receiver(tcp),
  SF = mk_sender(tcp,#ld{cache_connections=false}),
  ?assert(send_receive(PR,SF)),
  SF(close,''),
  SF(reset,#ld{cache_connections=false}).

subscriber_send_tcp_cache_test() ->
  PR0 = mk_receiver(tcp),
  SF = mk_sender(tcp,#ld{cache_connections=true}),
  ?assert(send_receive(PR0,SF)),
  SF(close,''),
  PR1 = mk_receiver(tcp),
  NSF = SF(reset,#ld{cache_connections=true}),
  ?assert(send_receive(PR1,NSF)),
  NSF(close,'').

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% test helpers

mk_receiver(Prot) ->
  spawn_monitor(
    case Prot of
      udp ->
        fun() ->
            Opts = [binary,{reuseaddr,true},{active,true}],
            {ok,_} = gen_udp:open(16#dada,Opts),
            receive {udp,_,{127,0,0,1},_,B}->exit(B)end
        end;
      tcp ->
        fun() ->
            Opts = [binary,{reuseaddr,true},{active,true}],
            {ok,ListenSock} = gen_tcp:listen(16#dada,Opts),
            {ok,Socket} = gen_tcp:accept(ListenSock),
            receive {tcp,Socket,B}->exit(B)end
        end
    end).

mk_sender(Prot,LD) ->
  mk_subscriber({Prot,{"localhost",16#dada}},"PWD",LD).

send_receive(PR,SF) ->
  SF(send,true),
  do_receive(PR).

do_receive({Pid,Ref}) ->
  receive {'DOWN',Ref,process,Pid,<<_:32,X/binary>>} ->
      binary_to_term(prf_crypto:decrypt("PWD",X))
  after 1000 -> false
  end.

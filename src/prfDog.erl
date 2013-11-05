%% -*- erlang-indent-level: 2 -*-
%%% Created : 17 Oct 2008 by Mats Cronqvist <masse@kreditor.se>

%% implements a proxy function between watchdog and the prf consumers.

-module(prfDog).
-author('Mats Cronqvist').

%% prf callbacks
-export([collect/1,config/2]).

% gen_serv callbacks
-export(
   [handle_info/2
    ,handle_call/3
    ,init/1
    ,rec_info/1]).

-include("log.hrl").

-record(ld,{args,acceptor,socket=[],msg=orddict:new(),cookie="I'm a Cookie"}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% prf callbacks, runs in the prfTarg process

collect(init) ->
  gen_serv:start(?MODULE),
  {gen_serv:get_state(?MODULE),{?MODULE,[]}};
collect(LD) ->
  {LD,{?MODULE,gen_server:call(?MODULE,get_data)}}.

config(LD,Data) ->
  ?log([unknown,{data,Data}]),
  LD.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% implementation details

sock_opts() -> [binary, {reuseaddr,true}, {active,true}, {packet,4}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_serv callbacks

rec_info(ld) -> record_info(fields,ld);
rec_info(_)  -> [].

init(Args) ->
  LD = #ld{args=Args,acceptor=accept(producer,56669,sock_opts())},
  watchdog:add_send_subscriber(udp,"localhost",56669,LD#ld.cookie),
  LD.

handle_call(get_data,_,LD) ->
  {LD#ld.msg,LD#ld{msg=orddict:new()}}.

%% M=fun({Proto,_,IP,PortNo,Payload})->{Proto,IP,PortNo,length(Payload)}end.
%% F=fun(G)->receive X -> erlang:display(M(X)), G(G) end end.
%% L=fun()->{ok,Sck}=gen_udp:open(16#BEBE,[{active,true}]),F(F)end.
%% register(udp_echo,spawn(L)).

handle_info({new_socket,producer,Sock},LD) ->
  %% we accepted a socket towards a producer.
%%  inet:setopts(Sock,[{active,once}]),
  LD#ld{socket=[Sock|LD#ld.socket]};
handle_info({tcp,Sock,Bin},LD) ->
  case lists:member(Sock,LD#ld.socket) of
    true ->
      %% got data from a known socket. this is good
      gen_tcp:close(Sock),
      {watchdog,Node,Trig,Msg} = prf_crypto:decrypt(LD#ld.cookie,Bin),
      LD#ld{socket=LD#ld.socket--[Sock],
            msg=orddict:store({Node,Trig},Msg,LD#ld.msg)};
    false->
      %% got data from unknown socket. wtf?
      ?log([{data_from,Sock},{sockets,LD#ld.socket},{bytes,byte_size(Bin)}]),
      LD
  end;
handle_info({tcp_closed, Sock},LD) ->
  LD#ld{socket=LD#ld.socket--[Sock]};
handle_info({tcp_error, Sock, Reason},LD) ->
  ?log([{tcp_error,Reason},{socket,Sock}]),
  LD;
handle_info(Msg,LD) ->
  ?log([{unrec,Msg}]),
  LD.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% accept is blocking, so it runs in its own process
accept(What,Port,Opts) ->
  erlang:spawn_link(fun() -> acceptor(What,Port,Opts) end).

acceptor(What,Port,Opts) ->
  {ok,ListenSock} = gen_tcp:listen(Port,Opts),
  acceptor_loop(What,ListenSock).

acceptor_loop(What,ListenSock) ->
  {ok,Socket} = gen_tcp:accept(ListenSock),
  ?MODULE ! {new_socket,What,Socket},
  gen_tcp:controlling_process(Socket,whereis(?MODULE)),
  acceptor_loop(What,ListenSock).

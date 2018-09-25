%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emq_store_redis).

-behaviour(gen_server).

-include_lib("emqttd/include/emqttd.hrl").

-include_lib("emqttd/include/emqttd_internal.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

-export([load/1, unload/0]).

%% API Function Exports
-export([start_link/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

%% Hooks functions

-export([on_session_subscribed/4, on_message_publish/2, on_message_acked/4]).

%% 
-export([read_messages/3, match_messages/3]).

-record(state, {redis_client, send_interval, send_timer, host, port, db, password}).

%% Called when the plugin application start
load(Env) ->
    emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

%% 启动一个线程来处理持久化消息的发生，完毕后线程自动退出
on_session_subscribed(_ClientId, _Username, {Topic, _Opts}, _Env) ->
    Time = erlang:timestamp(),
    io:format("extract for ~p~n", [Topic]),
    extract_messages(Topic, Time).

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #mqtt_message{topic = Topic}, _Env) ->
    io:format("publish ~s~n", [emqttd_message:format(Message)]),
    %%subs = lists:append(emqttd_router:match(Topic),emqttd_router:match_local(Topic)),
    store_message(Message),
    {ok, Message}.

on_message_acked(ClientId, Username, Message, _Env) ->
    io:format("client(~s/~s) acked: ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
    delete_message(Message),
    {ok, Message}.

%% Called when the plugin application stop
unload() ->
    emqttd:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/4).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
extract_messages(Topic, Deadline) ->
    gen_server:call(?MODULE, {extract, Topic, Deadline}).

store_message(Message) ->
    gen_server:call(?MODULE, {store, Message}).

delete_message(Message) ->
    gen_server:call(?MODULE, {delete, Message}).

%% @doc Start the retainer
-spec(start_link(Env :: list()) -> {ok, pid()} | ignore | {error, any()}).
start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Env], []).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Env]) ->
    io:format("env: ~p~n", [Env]),
    Host = proplists:get_value(host, Env, 'localhost'),
    Port = proplists:get_value(port, Env, 6379),
    Db = proplists:get_value(database, Env, 0),
    Password = proplists:get_value(password, Env, []),
    Retry = proplists:get_value(retry, Env, 1000),
    io:format("redis: ~p ~p ~p ~p ~p~n", [Host, Port, Db, Password, Retry]),
    {ok, C} = eredis:start_link(Host, Port, Db, Password, Retry),
    State = #state{redis_client = C, host = Host, port = Port, db = Db, password = Password},
    {ok, start_send_timer(proplists:get_value(send_interval, Env, 0), State)}.

start_send_timer(0, State) ->
    State#state{send_interval = 0, send_timer = undefined};
start_send_timer(undefined, State) ->
    State#state{send_interval = 0, send_timer = undefined};
start_send_timer(Ms, State) ->
    {ok, Timer} = timer:send_interval(Ms, send),
    State#state{send_interval = Ms, send_timer = Timer}.

handle_call({extract, Topic, Deadline}, _From, State) ->
    extract(Topic, Deadline, State),    
    {reply, ok, State};
handle_call({store, Message}, _From, State) ->
    store(Message, State),
    {reply, ok, State};
handle_call({delete, Message}, _From, State) ->
    delete(Message, State),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(send, State = #state{send_interval = Never})
    when Never =:= 0 orelse Never =:= undefined ->
    {noreply, State, hibernate};

handle_info(send, State = #state{send_interval = Interval}) ->
    %% 查询未应答消息，并publish

    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State = #state{redis_client = C, send_timer = TRef1}) ->
    timer:cancel(TRef1),
    eredis:stop(C).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

extract(Topic, Deadline, State = #state{host = Host, port = Port, db = Db, password = Password}) ->
    case emqttd_topic:wildcard(Topic) of
        false -> spawn(?MODULE, read_messages, [{Host, Port, Db, Password}, Topic, Deadline]);
        true  -> spawn(?MODULE, match_messages, [{Host, Port, Db, Password}, Topic, Deadline])
    end.

read_messages({Host, Port, Db, Password}, Topic, Deadline) ->
    {ok, Client} = eredis:start_link(Host, Port, Db, Password, 1000),
    scan_redis(Client, <<"0">>, make_pattern(Topic), 10, Deadline, read_send/3),
    eredis:stop(Client).

match_messages({Host, Port, Db, Password}, Topic, Deadline) ->
    ignore.

read_send(_Client, [], _) ->
    ignore;
read_send(Client, Keys, Deadline) ->
    lists:foreach(fun(Key) ->
        case eredis:q(Client, ["GET", Key]) of
            {ok, undefined} ->
                ignore;
            {ok, Bin} ->
                Message = binary_to_term(Bin),
                publish(Message);
                %case binary_to_term(Bin) of
                %    Message = #mqtt_message{topic = Topic, timestamp = Time} when -> 
                
                
            {_, _} ->
                ignore
        end end, Keys).

publish(Message = #mqtt_message{topic = Topic}) ->
    emqttd_pubsub:publish(Topic, Message), ok.
    

scan_redis(C, Start, Pattern, Count, Deadline, Fun) ->
    case eredis:q(C, ["SCAN", Start, "MATCH", Pattern, "COUNT", Count]) of
        {ok, [<<"0">>, Keys]} ->
            Fun(C, Keys, Deadline);
        {ok, [Cursor, Keys]} ->
            Fun(C, Keys, Deadline),
            scan_redis(C, Cursor, Pattern, Count, Deadline, Fun)
    end.

make_pattern(Topic) ->
    Space = <<":">>,
    P = <<"*">>,
    <<Topic/binary, Space/binary, P/binary>>.   

make_full_key(Topic, MsgId) ->
    Space = <<":">>,
    <<Topic/binary, Space/binary, MsgId/binary>>.

store(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _State) ->
    ok;
store(Message = #mqtt_message{topic = Topic, id = MsgId}, State = #state{redis_client = Client}) ->
    io:format("save: msgId ~p -> ~p~n", [MsgId, Topic]),
    Key = make_full_key(Topic, MsgId),
    {ok, <<"OK">>} = eredis:q(Client, ["SET", Key, term_to_binary(Message)]),
    ok.

delete(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _State) ->
    ok;
delete(Message = #mqtt_message{topic = Topic, id = MsgId}, State = #state{redis_client = Client}) ->
    io:format("delete: msgId ~p -> ~p~n", [MsgId, Topic]),
    Key = make_full_key(Topic, MsgId),
    case eredis:q(Client, ["DEL", Key]) of
        {ok, _} ->
            ok;
        _ ->
            ignore
    end.

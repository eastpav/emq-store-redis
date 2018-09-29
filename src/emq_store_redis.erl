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
-export([read_messages/4, match_messages/4]).

-record(state, {redis_client, read_interval, host, port, db, password}).

%% Called when the plugin application start
load(Env) ->
    emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

%% 启动一个线程来处理持久化消息的发生，完毕后线程自动退出
on_session_subscribed(_ClientId, _Username, {Topic, _Opts}, _Env) ->
    Time = erlang:timestamp(),
    extract_messages(Topic, timestamp_to_system_time(Time)).

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};
on_message_publish(Message = #mqtt_message{qos = 0}, _Env) ->
    {ok, Message};
on_message_publish(Message, _Env) ->
    lager:debug("on publish ~s~n", [emqttd_message:format(Message)]),
    store_message(Message),
    {ok, Message}.

on_message_acked(_ClientId, _Username, Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};
on_message_acked(_ClientId, _Username, Message = #mqtt_message{qos = 0}, _Env) ->
    {ok, Message};
on_message_acked(ClientId, Username, Message, _Env) ->
    lager:debug("client(~s/~s) acked: ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
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

%% 启动redis进程池来处理消息的存储
init([Env]) ->
    Host = proplists:get_value(host, Env, 'localhost'),
    Port = proplists:get_value(port, Env, 6379),
    Db = proplists:get_value(database, Env, 0),
    Password = proplists:get_value(password, Env, []),
    Retry = proplists:get_value(retry, Env, 1000),
    lager:info("redis info: ~p ~p ~p ~p~n", [Host, Port, Db, Retry]),
    {ok, C} = eredis:start_link(Host, Port, Db, Password, Retry),
    State = #state{redis_client = C, 
                   read_interval = proplists:get_value(read_interval, Env, 0), 
                   host = Host, 
                   port = Port, 
                   db = Db, 
                   password = Password},
    {ok, State}.

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

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State = #state{redis_client = C}) ->
    eredis:stop(C).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

extract(Topic, Deadline, #state{read_interval = Read_interval, host = Host, port = Port, db = Db, password = Password}) ->
    case emqttd_topic:wildcard(Topic) of
        false -> spawn(?MODULE, read_messages, [{Host, Port, Db, Password}, Topic, Deadline, Read_interval]);
        true  -> spawn(?MODULE, match_messages, [{Host, Port, Db, Password}, Topic, Deadline, Read_interval])
    end.

read_messages({Host, Port, Db, Password}, Topic, Deadline, Read_interval) ->
    {ok, Client} = eredis:start_link(Host, Port, Db, Password, 1000),
    scan_sets(Client, Topic, 0, Deadline, 10, Read_interval),
    eredis:stop(Client).

match_messages({_Host, _Port, _Db, _Password}, _Topic, _Deadline, _Read_interval) ->
    ignore.

publish(Message = #mqtt_message{topic = Topic, timestamp = Timestamp}) ->
    emqttd_pubsub:publish(Topic, Message), 
    timestamp_to_system_time(Timestamp).
    
%%
%% 根据scope对SortedSet进行遍历，起始值为0，截止值为当前时间（微秒）
%% LIMIT只限制每次查询的条数，若使用LIMIT来遍历，会出现有些消息无法被遍历到。
%% 原因是遍历后会publish，客户端应答后记录会被删除，导致LIMIT的索引失效。
%%
scan_sets(C, Key, Start, Deadline, Count, Interval) ->
    case eredis:q(C, ["ZRANGEBYSCORE", Key, to_scope(Start), integer_to_binary(Deadline), "LIMIT", 0, Count]) of
        {ok, []} ->
            ok;
        {ok, Members} ->
            Upper = 
            lists:foldl(fun(M, Acc) -> 
                                case eredis:q(C, ["GET", M]) of
                                    {ok, undefined} ->
                                        io:format("scan ignore~n"),
                                        Acc;
                                    {ok, Bin} ->
                                        Message = binary_to_term(Bin),
                                        publish(Message);
                                    {_, _} ->
                                        io:format("scan ignore~n"),
                                        Acc end end, 0, Members),
            timer:sleep(Interval),
            scan_sets(C, Key, Upper, Deadline, Count, Interval)
    end.

to_scope(Num) ->
    Bracket = <<"(">>,
    N = integer_to_binary(Num),
    <<Bracket/binary, N/binary>>.

%% 存储消息使用SortedSet+Hash，SortedSet用户消息排序，Hash存储具体的消息。
%% SortedSet和Hash之间使用mqtt_message.id关联(pktid，存储和应答的值不一致)
store(#mqtt_message{topic = <<"$SYS/", _/binary>>}, _State) ->
    ok;
store(Message = #mqtt_message{topic = Topic, timestamp = Timestamp, id = MsgId}, #state{redis_client = Client}) ->
    save_mix(Client, Topic, timestamp_to_system_time(Timestamp), MsgId, term_to_binary(Message)),
    ok.

save_kv(Client, Key, Value) ->
    {ok, <<"OK">>} = eredis:q(Client, ["SET", Key, Value]).

%% 使用Sorted Set和Hash实现排序和存储
save_mix(Client, Key, Scope, Member, Value) ->
    case eredis:q(Client, ["ZADD", Key, Scope, Member]) of
        {ok, <<"1">>} ->
            save_kv(Client, Member, Value);
        _ ->
            ignore
    end.


delete(#mqtt_message{topic = <<"$SYS/", _/binary>>}, _State) ->
    ok;
delete(#mqtt_message{topic = Topic, id = MsgId}, #state{redis_client = Client}) ->
    remove_mix(Client, Topic, MsgId).

remove_kv(Client, Key) ->
    case eredis:q(Client, ["DEL", Key]) of
        {ok, _} ->
            ok;
        _ ->
            ignore
    end.
remove_mix(Client, Key, Member) ->
    case eredis:q(Client, ["ZREM", Key, Member]) of
        {ok, <<"1">>} ->
            %io:format("delete ok~n"),
            remove_kv(Client, Member),
            ok;
        {ok, <<"0">>} ->
            %io:format("delete failed no such Member~n"),
            ignore;
        _ ->
            ignore
    end.

timestamp_to_system_time({MegaSecs, Secs, MicroSecs}) ->
    MegaSecs * 1000000000000 + Secs * 1000000 + MicroSecs.


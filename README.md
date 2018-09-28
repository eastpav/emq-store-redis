# emq-store-redis
EMQ redis 消息存储，消息被消费后被删除

# 参数配置
store.redis.host  redis服务地址
store.redis.port  redis服务端口
store.redis.database  redis服务数据库
store.redis.password  redis服务密码
store.redis.retry redis重连间隔（ms）
store.read_interval 消息块获取间隔（ms）

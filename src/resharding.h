#ifndef RESHARDING_H_INC
#define RESHARDING_H_INC

#define BACKEND_COUNT 16
#define BACKENDS "localhost:6301 localhost:6302 localhost:6303 localhost:6304 localhost:6305 localhost:6306 localhost:6307 localhost:6308 localhost:6309 localhost:6310 localhost:6311 localhost:6312 localhost:6313 localhost:6314 localhost:6315 localhost:6316"
#define PROXY_COMMANDS "SET SETNX SETEX APPEND DEL SETBIT SETRANGE INCR DECR RPUSH LPUSH RPUSHX LPUSHX LINSERT RPOP LPOP BRPOP BRPOPLPUSH BLPOP LSET LTRIM LREM RPOPLPUSH SADD SREM SMOVE SPOP ZADD ZINCRBY ZREM ZREMRANGEBYSCORE ZREMRANGEBYRANK HSET HSETNX HMSET HINCRBY HDEL INCRBY DECRBY GETSET MSET MSETNX EXPIRE EXPIREAT"
#define PROXY_COMMANDS_COUNT 44
typedef struct reshardingClient {
	    int fd;
	    char hostname[30];
	    int port;
} reshardingcli;

/* Functions */
int connectToBackends(void);
int getBackend(char *key);
int writeToBackend(struct reshardingClient *backend, char *command);
int process_command(char *key, char* command);
int process_client_command(redisClient *c);
int process_element(robj *key, robj *val);


/* Global arrays */
struct reshardingClient *backends[BACKEND_COUNT];
char *proxy_commands[PROXY_COMMANDS_COUNT];


#endif

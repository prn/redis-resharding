#include <string.h>
#include "sds.h"
#include "redis.h"
#include "resharding.h"



int reshardingInit(void) {
    /* Fill proxy command array */
    sds *argv;
    int argc = 0;
    int i = 0;
    argv = sdssplitlen(PROXY_COMMANDS, strlen(PROXY_COMMANDS), " ", 1, &argc);
    for (i = 0; i < argc; i++) {
        proxy_commands[i] = malloc(sizeof(argv[i]));
        strcpy(proxy_commands[i], argv[i]);
    }
    sdsfree(argv);

}

/* This dummy was borrowed from readQueryFromClient
 * src/networking.c 1029 line */
void readBackend(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisLog(REDIS_VERBOSE, "readBackend for fd = %d", fd);
    char buf[REDIS_IOBUF_LEN];
    int nread;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    nread = read(fd, buf, REDIS_IOBUF_LEN);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            redisLog(REDIS_VERBOSE, "Reading from backend: %s",strerror(errno));
            return;
        }
    } else if (nread == 0) {
        redisLog(REDIS_VERBOSE, "Client closed connection");
        return;
    }
}


int connectToBackends(void) { /* Fill backends array and connect to each of it*/
    sds *argv;
    sds *hp_argv;
    int argc = 0;
    int hp_argc = 0;
    int i = 0;
    argv = sdssplitlen(BACKENDS, strlen(BACKENDS), " ", 1, &argc);
    for (i = 0; i < argc; i++) {
        backends[i] = malloc(sizeof(struct reshardingClient));
        hp_argv = sdssplitlen(argv[i], strlen(argv[i]), ":", 1, &hp_argc);
        if (hp_argc == 1)
            backends[i]->port = 6379;
        else
            backends[i]->port = atoi(hp_argv[1]);
        //strcpy(backends[i]->hostname, hp_argv[0]);
	anetResolve(NULL, hp_argv[0], backends[i]->hostname);
        //sdsfree(hp_argv);
        redisLog(REDIS_NOTICE, "Backend %s:%d added", backends[i]->hostname, backends[i]->port);

        /* Connect to backend */
        backends[i]->fd = anetTcpNonBlockConnect(NULL, backends[i]->hostname, backends[i]->port);
        if (backends[i]->fd == -1) {
            sdsfree(argv);
            redisLog(REDIS_WARNING,"Unable to connect to BACKEND: %s", strerror(errno));
            return REDIS_ERR;
        }
        redisLog(REDIS_VERBOSE, "Connected with fd = %d", backends[i]->fd);

        if (aeCreateFileEvent(server.el, backends[i]->fd,AE_READABLE, readBackend, NULL) ==
            AE_ERR) {
            close(backends[i]->fd);
            sdsfree(argv);
            redisLog(REDIS_WARNING,"Can't create readable event for BACKEND");
            return REDIS_ERR;
        }

        if (syncWrite(backends[i]->fd, "PING\r\n", 6, server.repl_syncio_timeout) == -1) {
            sdsfree(argv);
            redisLog(REDIS_WARNING,"Unable to write: %s",
                strerror(errno));
            return REDIS_ERR;
        }
    }
    sdsfree(argv);
    return REDIS_OK;
}



int getBackend(char *key) {
    int i = 0;
    int resharding_hash = 1986;
    int len = strlen(key);
    int p = 0;

    for (i = 0; i < len; i++) {
//        redisLog(REDIS_VERBOSE,"Get hash %d. resharding_hash = %d, key[i] = %d", i, resharding_hash, key[i]);
        resharding_hash = (resharding_hash * 113 + key[i]) % 6271;
    }

    p = resharding_hash % BACKEND_COUNT;
    redisLog(REDIS_VERBOSE,"Return ptr to backend with id %d (key %s, resharding_hash %d)", p, key, resharding_hash);
    struct reshardingClient *backend = backends[p];
    return backend;

}


int isProxyCommand(char *c) {
    /* return 0 if "c" is command for proxy */
    int i = 0;
    for (i = 0; i<PROXY_COMMANDS_COUNT; i++) {
        if (strcasecmp(c, proxy_commands[i]) == 0) {
            return 0;
        }
    }
    return 1;
}


int writeToBackend(reshardingcli *backend, char *command) {
    //redisLog(REDIS_VERBOSE, "write '%s' to backend %s:%d (%d)", command, backend->hostname, backend->port, backend->fd);
    int l = strlen(command);
    if (syncWrite(backend->fd, command, l, server.repl_syncio_timeout) == -1) {
        redisLog(REDIS_WARNING,"Unable write to %s:%d (%d): %s", backend->hostname, backend->port, backend->fd,
             strerror(errno));
        redisLog(REDIS_WARNING,"at command %s", command);
        return REDIS_ERR;
    }
    return REDIS_OK;
}


int first_use_process_command = 1;
int process_command(char *key, char* command) {
    if (first_use_process_command == 1) {
        redisLog(REDIS_WARNING, "Start use process_command");
        first_use_process_command = 0;
    }
    redisLog(REDIS_VERBOSE, "### resharding process_command %s", command);
    reshardingcli *backend = getBackend(key);
    writeToBackend(backend, command);
    writeToBackend(backend, "\r\n");
    return REDIS_OK;
}


int first_use_process_client_command = 1;
int process_client_command(redisClient *c) {
    if (first_use_process_client_command == 1) {
        redisLog(REDIS_WARNING, "Start use process_client_command");
        first_use_process_client_command = 0;
    }
    redisLog(REDIS_VERBOSE, "### resharding process_client_command");
    if (c->argc <= 1) {
        redisLog(REDIS_VERBOSE, "Too short command (argc: %d). Return from sharding", c->argc);
        return REDIS_OK;
    }
    reshardingcli *backend = getBackend((char*)c->argv[1]->ptr);
    int i = 0;
    for (i=0; i < c->argc; i++) {
        if (i != 0)
            writeToBackend(backend, " ");
        writeToBackend(backend, c->argv[i]->ptr);
    }
    writeToBackend(backend, "\r\n");
    return REDIS_OK;
}


/**
 * Translate key & val to string command and pass it to backends
 * over call process_command function
 */
int first_use_process_element = 1;
int process_element(robj *key, robj *val)
{
    if (first_use_process_element == 1) {
        redisLog(REDIS_WARNING, "Start use process_element");
        first_use_process_element = 0;
    }
    char buf[200];
    char kbuf[60];
    char vbuf[140];
    robj *ele;
    robj *obj;
    robj *obj2;
    listTypeIterator *li;
    setTypeIterator *si;
    hashTypeIterator *hi;
    char fav_key_prefix[4] = "f:i:";
    redisLog(REDIS_VERBOSE, "Proccess element T=%d, key='%s'", val->type, key->ptr);

    switch(val->type)
    {
        case REDIS_STRING:
            // cli> SET key val
            if (val->encoding == REDIS_ENCODING_INT) {
                snprintf(buf, sizeof(buf), "SET %s %ld", key->ptr, val->ptr);
            } else {
                snprintf(buf, sizeof(buf), "SET %s %s", key->ptr, (char*)val->ptr);
            }
            process_command(key->ptr, buf);
            break;

        case REDIS_LIST:
            li = listTypeInitIterator(val,0,REDIS_TAIL);
            listTypeEntry entry;
            while(listTypeNext(li,&entry)) {
                // cli> LPUSH key item
                ele = listTypeGet(&entry);
                if (ele->encoding == REDIS_ENCODING_INT) {
                    snprintf(buf, sizeof(buf), "LPUSH %s %ld", key->ptr, ele->ptr);
                } else {
                    snprintf(buf, sizeof(buf), "LPUSH %s %s", key->ptr, (char*)ele->ptr);
                }
                redisLog(REDIS_VERBOSE, "command '%s'", buf);
                process_command(key->ptr, buf);
                if (ele->refcount > 0) {
                    decrRefCount(ele);
                }
            }
            listTypeReleaseIterator(li);
            break;

	case REDIS_SET:
            if (strncmp(fav_key_prefix, (char*)key->ptr, 4) == 0) {
                return REDIS_OK;
            }
            si = setTypeInitIterator(val);
            while((ele = setTypeNextObject(si)) != NULL) {
                // cli> SADD key item
                if (ele->encoding == REDIS_ENCODING_INT) {
                    snprintf(buf, sizeof(buf), "SADD %s %ld", key->ptr, ele->ptr);
                } else {
                    snprintf(buf, sizeof(buf), "SADD %s %s", key->ptr, (char*)ele->ptr);
                }

                redisLog(REDIS_VERBOSE, "command '%s'", buf);
                process_command(key->ptr, buf);
                if (ele->refcount > 0) {
                    decrRefCount(ele);
                }
            }
            setTypeReleaseIterator(si);
            break;

        case REDIS_HASH:
            hi = hashTypeInitIterator(val);
            while (hashTypeNext(hi) != REDIS_ERR) {
                // cli> HSET key hkey item
                obj = hashTypeCurrentObject(hi, REDIS_HASH_KEY);
                if (obj->encoding == REDIS_ENCODING_INT) {
                    snprintf(kbuf, sizeof(kbuf), "%ld", obj->ptr);
                } else {
                    snprintf(kbuf, sizeof(kbuf), "%s", (char*)obj->ptr);
                }
                obj2 = hashTypeCurrentObject(hi, REDIS_HASH_VALUE);
                if (obj2->encoding == REDIS_ENCODING_INT) {
                    snprintf(vbuf, sizeof(vbuf), "%ld", obj2->ptr);
                } else {
                    snprintf(vbuf, sizeof(vbuf), "%s", (char*)obj2->ptr);
                }
                decrRefCount(obj);
                decrRefCount(obj2);

                snprintf(buf, sizeof(buf), "HSET %s %s %s", (char*)key->ptr, kbuf, vbuf);

                redisLog(REDIS_VERBOSE, "command '%s'", buf);
                process_command((char*)key->ptr, buf);

                memset(buf, 0, sizeof(buf));
                memset(kbuf, 0, sizeof(kbuf));
                memset(vbuf, 0, sizeof(vbuf));
            }
            hashTypeReleaseIterator(hi);
            break;

        case REDIS_ZSET:
            if (val->encoding == REDIS_ENCODING_SKIPLIST) {
                zset *zs = val->ptr;
                dictIterator *di = dictGetIterator(zs->dict);
                dictEntry *de;
                while((de = dictNext(di)) != NULL) {
                    // cli> ZADD key score item
                    robj *eleobj = dictGetEntryKey(de);
                    double *score = dictGetEntryVal(de);
                    if (eleobj->encoding == REDIS_ENCODING_INT) {
                        snprintf(buf, sizeof(buf), "ZADD %s %.1f %d", key->ptr, *score, eleobj->ptr);
                    } else {
                        snprintf(buf, sizeof(buf), "ZADD %s %.1f %s", key->ptr, *score, (char*)eleobj->ptr);
                    }
                    redisLog(REDIS_VERBOSE, "Command '%s'", buf);
                    process_command((char*)key->ptr, buf);
                    memset(buf, 0, sizeof(buf));
                    //decrRefCount(eleobj);
                }
                dictReleaseIterator(di);
            } else if (val->encoding == REDIS_ENCODING_ZIPLIST) {
                unsigned char *zl = val->ptr;
                unsigned char *eptr, *sptr;
                unsigned char *vstr;
                unsigned int vlen;
                long long vlong;

                eptr = ziplistIndex(zl,0);
                sptr = ziplistNext(zl,eptr);
                while (eptr != NULL) {
                    double score = zzlGetScore(sptr);
                    ziplistGet(eptr,&vstr,&vlen,&vlong);
                    if (vstr == NULL) {
                        snprintf(buf, sizeof(buf), "ZADD %s %.1f %d", key->ptr, score, vlong);
                    } else {
                        snprintf(buf, sizeof(buf), "ZADD %s %.1f %s", key->ptr, score, (char*)vstr);
                    }
                    redisLog(REDIS_VERBOSE, "Command ZIP '%s'", buf);
                    process_command((char*)key->ptr, buf);
                    memset(buf, 0, sizeof(buf));
                    zzlNext(zl,&eptr,&sptr);
                }
            }
            break;
    }
    memset(buf, 0, sizeof(buf));

    return REDIS_OK;
}

/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include <math.h> /* isnan(), isinf() */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>


/*-----------------------------------------------------------------------------
 * KVDB Commands
 *----------------------------------------------------------------------------*/

int dbgetGenericCommand(redisClient *c) {
	char *key = (char *)c->argv[1]->ptr;
	rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
 	size_t len = 0;
	char *err = NULL;
	char *returned_value;
  	returned_value = rocksdb_get(server.kvdb, readoptions, key, strlen(key), &len, &err);
	if (returned_value == NULL || len == 0) {
    		addReply(c, shared.nullbulk);
		return REDIS_OK;
	}
	robj *o = createObject(REDIS_STRING, returned_value);
    	addReplyBulk(c, o);
    	return REDIS_OK;
}

void dbgetCommand(redisClient *c) {
	dbgetGenericCommand(c);
}

void dbputGenericCommand(redisClient *c, robj *ok_reply) {
	rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
	char *key = (char *)c->argv[1]->ptr;
 	char *value = (char *)c->argv[2]->ptr;
	char *err = NULL;
	rocksdb_put(server.kvdb, writeoptions, key, strlen(key), value, strlen(value) + 1, &err);
	assert(!err);
	addReply(c, ok_reply ? ok_reply : shared.ok);
}

void dbputCommand(redisClient *c) {
        dbputGenericCommand(c, NULL);
}

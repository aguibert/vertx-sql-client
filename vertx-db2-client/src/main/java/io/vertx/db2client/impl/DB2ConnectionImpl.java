/*
 * Copyright (C) 2019,2020 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.db2client.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.db2client.DB2ConnectOptions;
import io.vertx.db2client.DB2Connection;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.SqlConnectionImpl;

public class DB2ConnectionImpl extends SqlConnectionImpl<DB2ConnectionImpl> implements DB2Connection {

    public static Future<DB2Connection> connect(Vertx vertx, DB2ConnectOptions options) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        DB2ConnectionFactory client;
        try {
          client = new DB2ConnectionFactory(vertx, options);
        } catch (Exception e) {
          return ctx.failedFuture(e);
        }
        Promise<DB2Connection> promise = ctx.promise();
        ctx.dispatch(null, v -> connect(client, ctx, promise));
        return promise.future();
      }

      private static void connect(DB2ConnectionFactory client, ContextInternal ctx, Promise<DB2Connection> promise) {
        client.connect(ctx)
          .map(conn -> {
            DB2ConnectionImpl mySQLConnection = new DB2ConnectionImpl(client, ctx, conn);
            conn.init(mySQLConnection);
            return (DB2Connection) mySQLConnection;
          }).onComplete(promise);
      }

    public DB2ConnectionImpl(DB2ConnectionFactory factory, ContextInternal context, Connection conn) {
        super(context, conn);
    }

    @Override
    public void handleNotification(int processId, String channel, String payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DB2Connection ping(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException("Ping command not implemented");
//        PingCommand cmd = new PingCommand();
//        cmd.handler = handler;
//        schedule(cmd);
//        return this;
    }

    @Override
    public Future<Void> ping() {
        throw new UnsupportedOperationException("Ping command not implemented");
    }

    @Override
    public DB2Connection debug(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException("Debug command not implemented");
    }

    @Override
    public Future<Void> debug() {
        throw new UnsupportedOperationException("Debug command not implemented");
    }
}

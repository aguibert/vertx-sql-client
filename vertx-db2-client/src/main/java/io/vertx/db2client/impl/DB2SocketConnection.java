/*
 * Copyright (C) 2017 Julien Viet
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
 *
 */

package io.vertx.db2client.impl;

import java.util.Map;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.NetSocketInternal;
import io.vertx.db2client.impl.codec.MySQLCodec;
import io.vertx.db2client.impl.command.InitialHandshakeCommand;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.SocketConnectionBase;
import io.vertx.sqlclient.impl.command.CommandResponse;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class DB2SocketConnection extends SocketConnectionBase {

  private MySQLCodec codec;

  public DB2SocketConnection(NetSocketInternal socket,
                               boolean cachePreparedStatements,
                               int preparedStatementCacheSize,
                               int preparedStatementCacheSqlLimit,
                               Context context) {
    super(socket, cachePreparedStatements, preparedStatementCacheSize, preparedStatementCacheSqlLimit, 1, context);
  }

  void sendStartupMessage(String username,
                          String password,
                          String database,
                          Map<String, String> properties,
                          Handler<? super CommandResponse<Connection>> completionHandler) {
    InitialHandshakeCommand cmd = new InitialHandshakeCommand(this, username, password, database, properties);
    cmd.handler = completionHandler;
    schedule(cmd);
  }

  @Override
  public void init() {
    codec = new MySQLCodec(this);
    ChannelPipeline pipeline = socket.channelHandlerContext().pipeline();
    pipeline.addBefore("handler", "codec", codec);
    super.init();
  }

  public void upgradeToSSLConnection(Handler<AsyncResult<Void>> completionHandler) {
    // Workaround for Vert.x 3.x
    ChannelPipeline pipeline = socket.channelHandlerContext().pipeline();
    Promise<Void> upgradePromise = Promise.promise();
    upgradePromise.future().setHandler(ar->{
      if (ar.succeeded()) {
        completionHandler.handle(Future.succeededFuture());
      } else {
        Throwable cause = ar.cause();
        if (cause instanceof DecoderException) {
          DecoderException err = (DecoderException) cause;
          cause = err.getCause();
        }
        completionHandler.handle(Future.failedFuture(cause));
      }
    });
    pipeline.addFirst("initiate-ssl-handler", new InitiateSslHandler(this, upgradePromise));
  }
}

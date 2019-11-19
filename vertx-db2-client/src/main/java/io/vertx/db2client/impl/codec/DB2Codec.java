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
package io.vertx.db2client.impl.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.vertx.db2client.impl.DB2SocketConnection;

import java.util.ArrayDeque;

public class DB2Codec extends CombinedChannelDuplexHandler<DB2Decoder, DB2Encoder> {

  private final ArrayDeque<CommandCodec<?, ?>> inflight = new ArrayDeque<>();

  public DB2Codec(DB2SocketConnection mySQLSocketConnection) {
    DB2Encoder encoder = new DB2Encoder(inflight, mySQLSocketConnection);
    DB2Decoder decoder = new DB2Decoder(inflight, encoder);
    init(decoder, encoder);
  }
  
  public static void dumpBuffer(ByteBuf buffer) {
      dumpBuffer(buffer, buffer.readableBytes());
  }
  
  public static void dumpBuffer(ByteBuf buffer, int length) {
      System.out.print(buffer.toString());
      ByteBuf copy = buffer.slice(buffer.readerIndex(), length);
      for (int i = 0; i < copy.writerIndex(); i++) {
          if (i % 16 == 0)
              System.out.print("\n  ");
          if (i % 8 == 0)
              System.out.print("    ");
          System.out.print(" ");
          System.out.print(String.format("%02x", copy.getByte(i)));
      }
      System.out.println();
  }
  
  public static void dumpBytes(byte[] bytes) {
      for (int i = 0; i < bytes.length; i++) {
          if (i % 16 == 0)
              System.out.print("\n  ");
          if (i % 8 == 0)
              System.out.print("    ");
          System.out.print(" ");
          System.out.print(String.format("%02x", bytes[i]));
      }
      System.out.println();
  }
}

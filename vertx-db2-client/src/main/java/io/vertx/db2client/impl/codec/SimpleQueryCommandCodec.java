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
import io.vertx.db2client.impl.drda.CCSIDManager;
import io.vertx.db2client.impl.drda.DRDAConnectRequest;
import io.vertx.db2client.impl.drda.DRDAQueryRequest;
import io.vertx.db2client.impl.drda.DRDARequest;
import io.vertx.db2client.impl.drda.Section;
import io.vertx.db2client.impl.drda.SectionManager;
import io.vertx.sqlclient.impl.command.CommandResponse;
import io.vertx.sqlclient.impl.command.SimpleQueryCommand;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;

class SimpleQueryCommandCodec<T> extends QueryCommandBaseCodec<T, SimpleQueryCommand<T>> {
    
    private final CCSIDManager ccsidManager = new CCSIDManager();

  SimpleQueryCommandCodec(SimpleQueryCommand<T> cmd) {
    super(cmd);
  }

  @Override
  void encode(DB2Encoder encoder) {
    super.encode(encoder);
    try {
        if (DRDAQueryRequest.isQuery(cmd.sql()))
            sendQueryCommand();
        else
            sendUpdateCommand();
    } catch(Throwable t) {
        t.printStackTrace();
        completionHandler.handle(CommandResponse.failure(t));
    }
  }
  
  private void sendUpdateCommand() {
      System.out.println("@AGG sending update command");
      ByteBuf packet = allocateBuffer();
      int packetStartIdx = packet.writerIndex();
      DRDAQueryRequest updateCommand = new DRDAQueryRequest(packet, ccsidManager);
      Section s = SectionManager.INSTANCE.getDynamicSection();
      updateCommand.writeExecuteImmediate(cmd.sql(), s, "quark_db"); // @AGG get DB name from config
      updateCommand.buildRDBCMM();
      updateCommand.completeCommand();
      
      // @AGG TODO: auto-generated keys chain an OPNQRY command
//      updateCommand.writeOpenQuery(s, 
//              "quark_db", 
//              0, // triggers default fetch size (64) to be used @AGG this should be configurable
//              ResultSet.TYPE_FORWARD_ONLY); // @AGG hard code to TYPE_FORWARD_ONLY

      sendPacket(packet, packet.writerIndex() - packetStartIdx);
  }

  private void sendQueryCommand() {
    ByteBuf packet = allocateBuffer();
    int packetStartIdx = packet.writerIndex();
    
    DRDAQueryRequest queryCommand = new DRDAQueryRequest(packet, ccsidManager);
    // not needed unless we are preparing special registers like setting a query timeout
    // EXCSQLSET
    // SQLSTT
    // EXCSQLSET
    // SQLSTT
    Section s = SectionManager.INSTANCE.getDynamicSection();
    queryCommand.writePrepareDescribeOutput(cmd.sql(), "quark_db", s); // @AGG get DB name from config
    // PRPSQLSTT
    // SQLATTR
    // SQLSTT
    
    // OPNQRY
    queryCommand.writeOpenQuery(s, 
            "quark_db", 
            0, // triggers default fetch size (64) to be used @AGG this should be configurable
            ResultSet.TYPE_FORWARD_ONLY); // @AGG hard code to TYPE_FORWARD_ONLY
    queryCommand.completeCommand();

    sendPacket(packet, packet.writerIndex() - packetStartIdx);
  }

}

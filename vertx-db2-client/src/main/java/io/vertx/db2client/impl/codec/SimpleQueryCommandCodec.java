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
import io.vertx.db2client.impl.drda.DRDAQueryRequest;
import io.vertx.db2client.impl.drda.DRDARequest;
import io.vertx.db2client.impl.drda.Section;
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
        sendQueryCommand();
    } catch(Throwable t) {
        t.printStackTrace();
        completionHandler.handle(CommandResponse.failure(t));
    }
  }

//  @Override
//  protected void handleInitPacket(ByteBuf payload) {
//    // may receive ERR_Packet, OK_Packet, LOCAL INFILE Request, Text Resultset
//    int firstByte = payload.getUnsignedByte(payload.readerIndex());
//    if (firstByte == OK_PACKET_HEADER) {
//      OkPacket okPacket = decodeOkPacketPayload(payload, StandardCharsets.UTF_8);
//      handleSingleResultsetDecodingCompleted(okPacket.serverStatusFlags(), (int) okPacket.affectedRows(), (int) okPacket.lastInsertId());
//    } else if (firstByte == ERROR_PACKET_HEADER) {
//      handleErrorPacketPayload(payload);
//    } else if (firstByte == 0xFB) {
//      payload.skipBytes(1);
//      String filename = readRestOfPacketString(payload, StandardCharsets.UTF_8);
//      sendFileWrappedInPacket(filename);
//      sendEmptyPacket();
//    } else {
//      handleResultsetColumnCountPacketBody(payload);
//    }
//  }

  private void sendQueryCommand() {
    ByteBuf packet = allocateBuffer();
    int packetStartIdx = packet.writerIndex();
    
    DRDAQueryRequest queryCommand = new DRDAQueryRequest(packet, ccsidManager);
    // not needed unless we are preparing special registers like setting a query timeout
    // EXCSQLSET
    // SQLSTT
    // EXCSQLSET
    // SQLSTT
    
    Section section = new Section("SYSSH200",// name,  @AGG guessing on the section name
            1, // sectionNumber, @AGG guessing section number
            "NULLID", //cursorName, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT, // resultSetHoldability, @AGG assume we are always  HOLD_CURSORS_OVER_COMMIT
            false); //isGenerated);
    queryCommand.writePrepareDescribeOutput(cmd.sql(), "quark_db", section); // @AGG get DB name from config
    // PRPSQLSTT
    // SQLATTR
    // SQLSTT
    
    // OPNQRY
    queryCommand.writeOpenQuery(section, 
            "quark_db", 
            0, // triggers default fetch size (64) to be used @AGG this should be configurable
            ResultSet.TYPE_FORWARD_ONLY); // @AGG hard code to TYPE_FORWARD_ONLY

    sendPacket(packet, packet.writerIndex() - packetStartIdx);
  }

//  private void sendFileWrappedInPacket(String filePath) {
//    /*
//      We will try to use zero-copy file transfer in order to gain better performance.
//      File content needs to be wrapped in MySQL packets so we calculate the length of the file and then send a pre-calculated packet header with the content.
//     */
//    File file = new File(filePath);
//    long length = file.length();
//    // 16MB+ packet necessary?
//
//    ByteBuf packetHeader = allocateBuffer(4);
//    packetHeader.writeMediumLE((int) length);
//    packetHeader.writeByte(sequenceId++);
//    encoder.chctx.write(packetHeader);
//    encoder.socketConnection.socket().sendFile(filePath, 0);
//  }
//
//  private void sendEmptyPacket() {
//    ByteBuf packet = allocateBuffer(4);
//    // encode packet header
//    packet.writeMediumLE(0);
//    packet.writeByte(sequenceId);
//
//    sendNonSplitPacket(packet);
//  }
}

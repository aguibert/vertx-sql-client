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
import io.vertx.db2client.impl.drda.ColumnMetaData;
import io.vertx.db2client.impl.drda.Cursor;
import io.vertx.db2client.impl.drda.DRDAQueryRequest;
import io.vertx.db2client.impl.drda.DRDAQueryResponse;
import io.vertx.db2client.impl.drda.Section;
import io.vertx.db2client.impl.drda.SectionManager;
import io.vertx.sqlclient.impl.PreparedStatement;
import io.vertx.sqlclient.impl.command.CommandResponse;
import io.vertx.sqlclient.impl.command.PrepareStatementCommand;

class PrepareStatementCodec extends CommandCodec<PreparedStatement, PrepareStatementCommand> {
    
    private static enum CommandHandlerState {
        INIT,
        HANDLING_PARAM_COLUMN_DEFINITION,
        PARAM_DEFINITIONS_DECODING_COMPLETED,
        HANDLING_COLUMN_COLUMN_DEFINITION,
        COLUMN_DEFINITIONS_DECODING_COMPLETED
      }

  private CommandHandlerState commandHandlerState = CommandHandlerState.INIT;
  private ColumnMetaData columnDescs;
  private final CCSIDManager ccsidManager = new CCSIDManager();
  private Section section;
  
  // OVERALL PS FLOW
  // java: con.prepareStatement("SELECT * FROM users WHERE id=?");
  // java: ps.executeQuery();
  // send: ((opt) EXCSQLSET | SQLSTT  | EXCSQLSTT | ) PRPSQLSTT | SQLATTR  | SQLSTT   | DSCSQLSTT | OPNQRY  | SQLDTA
  // recv: ((opt) SQLCARD   | SQLCARD |             ) SQLDARD   | SQLDARD  | OPNQRYRM | QRYDSC    | QRYDTA  | ENDQRYRM | SQLCARD
  // java: con.close();
  // send: RDBCMM
  // recv: ENDUOWRM | SQLCARD | MONITORRD
  
  PrepareStatementCodec(PrepareStatementCommand cmd) {
    super(cmd);
  }

  @Override
  void encode(DB2Encoder encoder) {
    super.encode(encoder);
    sendStatementPrepareCommand();
  }
  
  private void sendStatementPrepareCommand() {
      System.out.println("@AGG PS encode");
      ByteBuf packet = allocateBuffer();
      // encode packet header
      int packetStartIdx = packet.writerIndex();
      DRDAQueryRequest prepareCommand = new DRDAQueryRequest(packet, ccsidManager);
      section = SectionManager.INSTANCE.getDynamicSection();
      String dbName = "quark_db"; // TODO: @AGG get this from config
      prepareCommand.writePrepareDescribeOutput(cmd.sql(), dbName, section);
      prepareCommand.writeDescribeInput(section, dbName);
      prepareCommand.completeCommand();

      // set payload length
      int payloadLength = packet.writerIndex() - packetStartIdx;
      sendPacket(packet, payloadLength);
    }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
      System.out.println("@AGG inside PS decode");
      switch (commandHandlerState) {
          case INIT:
              DRDAQueryResponse response = new DRDAQueryResponse(payload, ccsidManager);
              response.readPrepareDescribeInputOutput();
              ColumnMetaData columnMd = response.getColumnMetaData();
              columnDescs = columnMd;
              handleColumnDefinitionsDecodingCompleted();
              commandHandlerState = CommandHandlerState.COLUMN_DEFINITIONS_DECODING_COMPLETED;
              break;
          default: 
              throw new IllegalStateException("Unknown state: " + commandHandlerState);
      }
//    switch (commandHandlerState) {
//      case INIT:
//        int firstByte = payload.getUnsignedByte(payload.readerIndex());
//        if (firstByte == ERROR_PACKET_HEADER) {
//          handleErrorPacketPayload(payload);
//        } else {
//          // handle COM_STMT_PREPARE response
//          payload.readUnsignedByte(); // 0x00: OK
//          long statementId = payload.readUnsignedIntLE();
//          int numberOfColumns = payload.readUnsignedShortLE();
//          int numberOfParameters = payload.readUnsignedShortLE();
//          payload.readByte(); // [00] filler
//          int numberOfWarnings = payload.readShortLE();
//
//          // handle metadata here
//          this.statementId = statementId;
//          this.paramDescs = new ColumnDefinition[numberOfParameters];
//          this.columnDescs = new ColumnDefinition[numberOfColumns];
//
//          if (numberOfParameters != 0) {
//            processingIndex = 0;
//            this.commandHandlerState = CommandHandlerState.HANDLING_PARAM_COLUMN_DEFINITION;
//          } else if (numberOfColumns != 0) {
//            processingIndex = 0;
//            this.commandHandlerState = CommandHandlerState.HANDLING_COLUMN_COLUMN_DEFINITION;
//          } else {
//            handleReadyForQuery();
//            resetIntermediaryResult();
//          }
//        }
//        break;
//      case HANDLING_PARAM_COLUMN_DEFINITION:
//        paramDescs[processingIndex++] = decodeColumnDefinitionPacketPayload(payload);
//        if (processingIndex == paramDescs.length) {
//          if (isDeprecatingEofFlagEnabled()) {
//            // we enabled the DEPRECATED_EOF flag and don't need to accept an EOF_Packet
//            handleParamDefinitionsDecodingCompleted();
//          } else {
//            // we need to decode an EOF_Packet before handling rows, to be compatible with MySQL version below 5.7.5
//            commandHandlerState = CommandHandlerState.PARAM_DEFINITIONS_DECODING_COMPLETED;
//          }
//        }
//        break;
//      case PARAM_DEFINITIONS_DECODING_COMPLETED:
//        skipEofPacketIfNeeded(payload);
//        handleParamDefinitionsDecodingCompleted();
//        break;
//      case HANDLING_COLUMN_COLUMN_DEFINITION:
//        columnDescs[processingIndex++] = decodeColumnDefinitionPacketPayload(payload);
//        if (processingIndex == columnDescs.length) {
//          if (isDeprecatingEofFlagEnabled()) {
//            // we enabled the DEPRECATED_EOF flag and don't need to accept an EOF_Packet
//            handleColumnDefinitionsDecodingCompleted();
//          } else {
//            // we need to decode an EOF_Packet before handling rows, to be compatible with MySQL version below 5.7.5
//            commandHandlerState = CommandHandlerState.COLUMN_DEFINITIONS_DECODING_COMPLETED;
//          }
//        }
//        break;
//      case COLUMN_DEFINITIONS_DECODING_COMPLETED:
//        handleColumnDefinitionsDecodingCompleted();
//        break;
//    }
  }

  private void handleReadyForQuery() {
    completionHandler.handle(CommandResponse.success(new DB2PreparedStatement(
      cmd.sql(),
      new DB2ParamDesc(columnDescs),
      new DB2RowDesc(columnDescs),
      section)));
  }

  private void resetIntermediaryResult() {
    commandHandlerState = CommandHandlerState.INIT;
    columnDescs = null;
  }

  private void handleParamDefinitionsDecodingCompleted() {
    if (columnDescs.columns_ == 0) {
      handleReadyForQuery();
      resetIntermediaryResult();
    } else {
      this.commandHandlerState = CommandHandlerState.HANDLING_COLUMN_COLUMN_DEFINITION;
    }
  }

  private void handleColumnDefinitionsDecodingCompleted() {
    handleReadyForQuery();
    resetIntermediaryResult();
  }


}

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

import java.util.ArrayList;

import com.ibm.db2.jcc.am.ResultSet;

import io.netty.buffer.ByteBuf;
import io.vertx.db2client.impl.codec.QueryCommandBaseCodec.CommandHandlerState;
import io.vertx.db2client.impl.drda.DRDAQueryRequest;
import io.vertx.db2client.impl.drda.DRDAQueryResponse;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.RowDesc;
import io.vertx.sqlclient.impl.command.CommandResponse;
import io.vertx.sqlclient.impl.command.ExtendedQueryCommand;


class ExtendedQueryCommandCodec<R> extends ExtendedQueryCommandBaseCodec<R, ExtendedQueryCommand<R>> {
  ExtendedQueryCommandCodec(ExtendedQueryCommand<R> cmd) {
    super(cmd);
    if (cmd.fetch() > 0) {
      // restore the state we need for decoding fetch response
      columnDefinitions = statement.rowDesc.columnDefinitions();
    }
    // @AGG always carry over column defs?
    columnDefinitions = statement.rowDesc.columnDefinitions();
    System.out.println("@AGG colDefs are: " + columnDefinitions + " on object " + this);
  }

  @Override
    void encode(DB2Encoder encoder) {
        System.out.println("@AGG extended query encode");
        super.encode(encoder);
        System.out.println("@AGG statement: " + statement);
        if (!DRDAQueryRequest.isQuery(cmd.sql()))
            throw new UnsupportedOperationException("Update PS");
        ByteBuf packet = allocateBuffer();
        String dbName = "quark_db"; // TODO @AGG get db name from config
        int fetchSize = 0; // TODO @AGG get fetch size from DB
        DRDAQueryRequest openQuery = new DRDAQueryRequest(packet, ccsidManager);
        Tuple params = cmd.params();
        Object[] inputs = new Object[params.size()];
        for (int i = 0; i < params.size(); i++)
            inputs[i] = params.getValue(i);
        openQuery.writeOpenQuery(statement.section, dbName, fetchSize, ResultSet.TYPE_FORWARD_ONLY, inputs.length,
                statement.paramDesc.paramDefinitions(), inputs);
        openQuery.completeCommand();
        encoder.chctx.writeAndFlush(packet);

//    if (statement.isCursorOpen) {
//      decoder = new RowResultDecoder<>(cmd.collector(), statement.rowDesc);
//      sendStatementFetchCommand(statement.statementId, cmd.fetch());
//    } else {
//      if (cmd.fetch() > 0) {
//        //TODO Cursor_type is READ_ONLY?
//        sendStatementExecuteCommand(statement.statementId, statement.paramDesc.paramDefinitions(), sendType, cmd.params(), (byte) 0x01);
//      } else {
//        // CURSOR_TYPE_NO_CURSOR
//        sendStatementExecuteCommand(statement.statementId, statement.paramDesc.paramDefinitions(), sendType, cmd.params(), (byte) 0x00);
//      }
//    }
  }

  @Override
  void decodePayload(ByteBuf payload, int payloadLength) {
      System.out.println("@AGG extended query decode");
      if (!DRDAQueryRequest.isQuery(cmd.sql()))
          throw new UnsupportedOperationException("Decode update PS");
      
      DRDAQueryResponse resp = new DRDAQueryResponse(payload, ccsidManager);
      resp.setColumnMetaData(columnDefinitions);
//      resp.readPrepareDescribeOutput();
//      resp.readBeginOpenQuery();
      resp.readBeginOpenQuery();
      //resp.readExecute();
//      columnDefinitions = resp.getColumnMetaData();
      decoder = new RowResultDecoder<>(cmd.collector(), new DB2RowDesc(columnDefinitions), resp.getCursor(),
              resp);
      commandHandlerState = CommandHandlerState.HANDLING_ROW_DATA;
//      return;
      while (decoder.next()) {
          decoder.handleRow(columnDefinitions.columns_, payload);
      }
      if (decoder.isQueryComplete())
          decoder.cursor.setAllRowsReceivedFromServer(true);
      else
          throw new UnsupportedOperationException("Need to fetch more data from DB");
      commandHandlerState = CommandHandlerState.HANDLING_END_OF_QUERY;
//      decodeQuery(payload);
//      return;
      int updatedCount = 0; // @AGG hardcoded to 0
      R result;
      Throwable failure;
      int size;
      RowDesc rowDesc;
      failure = decoder.complete();
      result = decoder.result();
      rowDesc = decoder.rowDesc;
      size = decoder.size();
      decoder.reset();
      cmd.resultHandler().handleResult(updatedCount, size, rowDesc, result, failure);
      completionHandler.handle(CommandResponse.success(true));
      return;
//    if (statement.isCursorOpen) {
//      int first = payload.getUnsignedByte(payload.readerIndex());
//      if (first == ERROR_PACKET_HEADER) {
//        handleErrorPacketPayload(payload);
//      } else {
//        // decoding COM_STMT_FETCH response
//        handleRows(payload, payloadLength, super::handleSingleRow);
//      }
//    } else {
//      // decoding COM_STMT_EXECUTE response
//      if (cmd.fetch() > 0) {
//        switch (commandHandlerState) {
//          case INIT:
//            int first = payload.getUnsignedByte(payload.readerIndex());
//            if (first == ERROR_PACKET_HEADER) {
//              handleErrorPacketPayload(payload);
//            } else {
//              handleResultsetColumnCountPacketBody(payload);
//            }
//            break;
//          case HANDLING_COLUMN_DEFINITION:
//            handleResultsetColumnDefinitions(payload);
//            break;
//          case COLUMN_DEFINITIONS_DECODING_COMPLETED:
//            // accept an EOF_Packet when DEPRECATE_EOF is not enabled
//            skipEofPacketIfNeeded(payload);
//            // do not need a break clause here
//          case HANDLING_ROW_DATA_OR_END_PACKET:
//            handleResultsetColumnDefinitionsDecodingCompleted();
//            // need to reset packet number so that we can send a fetch request
//            this.sequenceId = 0;
//            // send fetch after cursor opened
//            decoder = new RowResultDecoder<>(cmd.collector(), statement.rowDesc);
//
//            statement.isCursorOpen = true;
//
//            sendStatementFetchCommand(statement.statementId, cmd.fetch());
//            break;
//          default:
//            throw new IllegalStateException("Unexpected state for decoding COM_STMT_EXECUTE response with cursor opening");
//        }
//      } else {
//        super.decodePayload(payload, payloadLength);
//      }
//    }
  }
}

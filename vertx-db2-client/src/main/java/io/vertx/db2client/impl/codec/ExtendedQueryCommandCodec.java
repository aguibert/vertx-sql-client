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

import com.ibm.db2.jcc.am.ResultSet;

import io.netty.buffer.ByteBuf;
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
    }

    @Override
    void encode(DB2Encoder encoder) {
        System.out.println("@AGG extended query encode");
        super.encode(encoder);
        System.out.println("@AGG statement: " + statement);
        if (!DRDAQueryRequest.isQuery(cmd.sql()))
            throw new UnsupportedOperationException("Update PS");
        ByteBuf packet = allocateBuffer();
        String dbName = encoder.socketConnection.database();
        int fetchSize = 0; // TODO @AGG get fetch size from config
        DRDAQueryRequest openQuery = new DRDAQueryRequest(packet, ccsidManager);
        Tuple params = cmd.params();
        Object[] inputs = new Object[params.size()];
        for (int i = 0; i < params.size(); i++)
            inputs[i] = params.getValue(i);
        openQuery.writeOpenQuery(statement.section, dbName, fetchSize, ResultSet.TYPE_FORWARD_ONLY, inputs.length,
                statement.paramDesc.paramDefinitions(), inputs);
        openQuery.completeCommand();
        encoder.chctx.writeAndFlush(packet);
    }

    @Override
    void decodePayload(ByteBuf payload, int payloadLength) {
        System.out.println("@AGG extended query decode");
        if (!DRDAQueryRequest.isQuery(cmd.sql()))
            throw new UnsupportedOperationException("Decode update PS");

        DRDAQueryResponse resp = new DRDAQueryResponse(payload, ccsidManager);
        resp.setColumnMetaData(columnDefinitions);
        resp.readBeginOpenQuery();
        decoder = new RowResultDecoder<>(cmd.collector(), new DB2RowDesc(columnDefinitions), resp.getCursor(), resp);
        commandHandlerState = CommandHandlerState.HANDLING_ROW_DATA;
        while (decoder.next()) {
            decoder.handleRow(columnDefinitions.columns_, payload);
        }
        if (decoder.isQueryComplete())
            decoder.cursor.setAllRowsReceivedFromServer(true);
        else
            throw new UnsupportedOperationException("Need to fetch more data from DB");
        commandHandlerState = CommandHandlerState.HANDLING_END_OF_QUERY;
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
    }
}

package io.vertx.db2client.impl.codec;

import java.sql.SQLException;
import java.util.stream.Collector;

import io.netty.buffer.ByteBuf;
import io.vertx.db2client.impl.DB2RowImpl;
import io.vertx.db2client.impl.drda.Cursor;
import io.vertx.db2client.impl.drda.DRDAQueryResponse;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.RowDecoder;

class RowResultDecoder<C, R> extends RowDecoder<C, R> {

  final DB2RowDesc rowDesc;
  final Cursor cursor;
  final DRDAQueryResponse response;

  RowResultDecoder(Collector<Row, C, R> collector, DB2RowDesc rowDesc, Cursor cursor, DRDAQueryResponse resp) {
    super(collector);
    this.rowDesc = rowDesc;
    this.cursor = cursor;
    this.response = resp;
  }
  
  public boolean isQueryComplete() {
      return response.isQueryComplete();
  }
  
  public boolean next() {
      response.readOpenQueryData();
      try {
        return cursor.next();
    } catch (SQLException e) {
        e.printStackTrace();
        return false;
    }
  }

    @Override
    protected Row decodeRow(int len, ByteBuf in) {
        Row row = new DB2RowImpl(rowDesc);
        for (int i = 1; i < rowDesc.columnDefinitions().columns_ + 1; i++) {
            Object o = cursor.getObject(i);
            System.out.println("  @AGG got value=" + o);
            row.addValue(o);
        }
//        String str = cursor.getString(1);
//        System.out.println("  @AGG got string value=" + str);
//        row.addString(str);
        return row;
    }
}


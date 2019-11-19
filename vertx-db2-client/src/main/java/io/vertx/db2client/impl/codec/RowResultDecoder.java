package io.vertx.db2client.impl.codec;

import java.nio.charset.Charset;
import java.util.stream.Collector;

import io.netty.buffer.ByteBuf;
import io.vertx.db2client.impl.DB2RowImpl;
import io.vertx.db2client.impl.drda.CCSIDManager;
import io.vertx.db2client.impl.drda.Cursor;
import io.vertx.db2client.impl.drda.DRDAQueryResponse;
import io.vertx.db2client.impl.drda.DRDARequest;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.RowDecoder;

class RowResultDecoder<C, R> extends RowDecoder<C, R> {
  private static final int NULL = 0xFB;

  final DB2RowDesc rowDesc;
  final Cursor cursor;
  final DRDAQueryResponse response;

  RowResultDecoder(Collector<Row, C, R> collector, DB2RowDesc rowDesc, Cursor cursor, DRDAQueryResponse resp) {
    super(collector);
    this.rowDesc = rowDesc;
    this.cursor = cursor;
    this.response = resp;
  }

  @Override
  protected Row decodeRow(int len, ByteBuf in) {
    Row row = new DB2RowImpl(rowDesc);
    System.out.println("@AGG decoding row len=" + len);
    System.out.println("@AGG decode row buffer:");
    DB2Codec.dumpBuffer(in);
    
    boolean readData = response.readOpenQueryData();
    System.out.println("@AGG did read data? " + readData);
    System.out.println("@AGG cursor bytes:");
    DB2Codec.dumpBuffer(cursor.dataBuffer_);
//    if (rowDesc.dataFormat() == DataFormat.BINARY) {
//      // BINARY row decoding
//      // 0x00 packet header
//      // null_bitmap
//      int nullBitmapLength = (len + 7 + 2) >>  3;
//      int nullBitmapIdx = 1 + in.readerIndex();
//      in.skipBytes(1 + nullBitmapLength);
//
//      // values
//      for (int c = 0; c < len; c++) {
//        int val = c + 2;
//        int bytePos = val >> 3;
//        int bitPos = val & 7;
//        byte mask = (byte) (1 << bitPos);
//        byte nullByte = (byte) (in.getByte(nullBitmapIdx + bytePos) & mask);
//        Object decoded = null;
//        if (nullByte == 0) {
//          // non-null
//          ColumnDefinition columnDef = rowDesc.columnDefinitions()[c];
//          DataType dataType = columnDef.type();
//          int collationId = rowDesc.columnDefinitions()[c].characterSet();
//          Charset charset = CCSIDManager.UTF8; // @AGG for now hardcode to UTF8 //Charset.forName(MySQLCollation.valueOfId(collationId).mappedJavaCharsetName());
//          int columnDefinitionFlags = columnDef.flags();
//          decoded = DataTypeCodec.decodeBinary(dataType, charset, columnDefinitionFlags, in);
//        }
//        row.addValue(decoded);
//      }
//    } else {
//      // TEXT row decoding
//      for (int c = 0; c < len; c++) {
//        Object decoded = null;
//        if (in.getUnsignedByte(in.readerIndex()) == NULL) {
//          in.skipBytes(1);
//        } else {
//          DataType dataType = rowDesc.columnDefinitions()[c].type();
//          int columnDefinitionFlags = rowDesc.columnDefinitions()[c].flags();
//          int collationId = rowDesc.columnDefinitions()[c].characterSet();
//          Charset charset = CCSIDManager.UTF8; // @AGG for now hardcode to UTF8 Charset.forName(MySQLCollation.valueOfId(collationId).mappedJavaCharsetName());
//          decoded = DataTypeCodec.decodeText(dataType, charset, columnDefinitionFlags, in);
//        }
//        row.addValue(decoded);
//      }
//    }
    return row;
  }
}


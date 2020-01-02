package io.vertx.db2client.impl.codec;

import java.util.ArrayDeque;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.vertx.sqlclient.impl.command.CommandResponse;

class DB2Decoder extends ByteToMessageDecoder {

    private final ArrayDeque<CommandCodec<?, ?>> inflight;

    DB2Decoder(ArrayDeque<CommandCodec<?, ?>> inflight) {
        this.inflight = inflight;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int payloadLength = computeLength(in);
        if (payloadLength >= DB2Codec.PACKET_PAYLOAD_LENGTH_LIMIT)
            throw new UnsupportedOperationException("TODO @AGG split package decoding not implemented");
        System.out.println("@AGG received " + payloadLength + " bytes for " + inflight.peek());
        decodePayload(in.readRetainedSlice(payloadLength), payloadLength, in.getShort(in.readerIndex() + 4));
    }
    
    private int computeLength(ByteBuf in) {
        int index = 0;
        boolean dssContinues = true;
        while (dssContinues && index < in.readableBytes()) {
            if (in.readableBytes() >= index + 3)
                dssContinues &= (in.getByte(index + 3) & 0x40) == 0x40;
            else
                dssContinues = false;
            index += in.getShort(index);
        }
        return index;
    }

    private void decodePayload(ByteBuf payload, int payloadLength, int sequenceId) {
        CommandCodec<?,?> ctx = inflight.peek();
        ctx.sequenceId = sequenceId + 1;
        try {
            ctx.decodePayload(payload, payloadLength);
        } catch (Throwable t) {
            t.printStackTrace();
            ctx.completionHandler.handle(CommandResponse.failure(t));
        }
        payload.clear();
        payload.release();
    }
}

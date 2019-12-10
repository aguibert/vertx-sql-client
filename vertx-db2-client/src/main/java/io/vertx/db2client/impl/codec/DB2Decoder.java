package io.vertx.db2client.impl.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.vertx.sqlclient.impl.command.CommandResponse;

import static io.vertx.db2client.impl.codec.Packets.*;

import java.util.ArrayDeque;
import java.util.List;

class DB2Decoder extends ByteToMessageDecoder {

    private final ArrayDeque<CommandCodec<?, ?>> inflight;
    private final DB2Encoder encoder;

    private CompositeByteBuf aggregatedPacketPayload = null;

    DB2Decoder(ArrayDeque<CommandCodec<?, ?>> inflight, DB2Encoder encoder) {
        this.inflight = inflight;
        this.encoder = encoder;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
//        InitialHandshakeCommandCodec.dumpBuffer(in);
        // TODO: do we need to handle split packets?
        int payloadLength = computeLength(in);
        System.out.println("@AGG received " + payloadLength + " bytes for " + inflight.peek());
        decodePayload(in.readRetainedSlice(payloadLength), payloadLength, in.getShort(in.readerIndex() + 4));
//        if (in.readableBytes() > 4) {
//            int packetStartIdx = in.readerIndex();
//            int payloadLength = in.readShort();
//            int sequenceId = in.readUnsignedByte();
//            System.out.println("@AGG length is: " + payloadLength);
//
//            if (payloadLength >= PACKET_PAYLOAD_LENGTH_LIMIT && aggregatedPacketPayload == null) {
//                aggregatedPacketPayload = ctx.alloc().compositeBuffer();
//            }
//
//            // payload
//            if (in.readableBytes() >= payloadLength) {
//                if (aggregatedPacketPayload != null) {
//                    // read a split packet
//                    aggregatedPacketPayload.addComponent(true, in.readRetainedSlice(payloadLength));
//                    sequenceId++;
//
//                    if (payloadLength < PACKET_PAYLOAD_LENGTH_LIMIT) {
//                        // we have just read the last split packet and there will be no more split
//                        // packet
//                        decodePayload(aggregatedPacketPayload, aggregatedPacketPayload.readableBytes(), sequenceId);
//                        aggregatedPacketPayload.release();
//                        aggregatedPacketPayload = null;
//                    }
//                } else {
//                    // read a non-split packet
//                    decodePayload(in.readSlice(payloadLength), payloadLength, sequenceId);
//                }
//            } else {
//                in.readerIndex(packetStartIdx);
//            }
//        }
    }
    
    private int computeLength(ByteBuf in) {
        int index = 0;
        boolean dssContinues = true;
        while (dssContinues && index < in.readableBytes()) {
            if (in.readableBytes() >= index + 3)
                dssContinues &= (in.getByte(index + 3) & 0x40) == 0x40;
            else
                dssContinues = false;
            int dssLen = in.getShort(index);
            index += in.getShort(index);
        }
        return index;
    }

    private void decodePayload(ByteBuf payload, int payloadLength, int sequenceId) {
        CommandCodec ctx = inflight.peek();
        ctx.sequenceId = sequenceId + 1; // TODO why increment by 1 here?
        int start = payload.readerIndex();
        try {
            ctx.decodePayload(payload, payloadLength);
        } catch (Throwable t) {
            t.printStackTrace();
            ctx.completionHandler.handle(CommandResponse.failure(t));
        }
        payload.clear();
        payload.release();
//        int readBytes = payload.readerIndex() - start;
//        if (readBytes != payloadLength)
//            System.out.println("WARNING: did not read all payload bytes: payload=" + payloadLength + " readBytes=" + readBytes + 
//                    " (" + (payloadLength - readBytes) + " remain)");
//        payload.discardReadBytes();
    }
}

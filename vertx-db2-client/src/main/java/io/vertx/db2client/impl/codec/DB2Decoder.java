package io.vertx.db2client.impl.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

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
        System.out.println("@AGG received bytes: " + in);
//        InitialHandshakeCommandCodec.dumpBuffer(in);
        // TODO: do we need to handle split packets?
        decodePayload(in, in.readableBytes(), in.getShort(in.readerIndex() + 4));
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

    private void decodePayload(ByteBuf payload, int payloadLength, int sequenceId) {
        CommandCodec ctx = inflight.peek();
        ctx.sequenceId = sequenceId + 1; // TODO why increment by 1 here?
        ctx.decodePayload(payload, payloadLength);
        payload.clear();
    }
}

package io.vertx.db2client.impl.drda;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.ArrayDeque;
import java.util.Deque;

import io.netty.buffer.ByteBuf;

public abstract class DRDAResponse {
    
    final ByteBuf buffer;
    final CCSIDManager ccsidManager;

    Deque<Integer> ddmCollectionLenStack = new ArrayDeque<>(4);
    private int ddmScalarLen_ = 0; // a value of -1 -> streamed ddm -> length unknown

    protected int dssLength_;
    private boolean dssIsContinued_;
    private boolean dssIsChainedWithSameID_;
    private int dssCorrelationID_ = 1;

    protected int peekedLength_ = 0;
    private int peekedCodePoint_ = END_OF_COLLECTION; // saves the peeked codept
    private int peekedNumOfExtendedLenBytes_ = 0;
    private int currentPos_ = 0;
//    private int pos_ = 0; // TODO: rename position fields

    final static int END_OF_COLLECTION = -1;
    final static int END_OF_SAME_ID_CHAIN = -2;

    public DRDAResponse(ByteBuf buffer, CCSIDManager ccsidManager) {
        this.buffer = buffer;
        this.ccsidManager = ccsidManager;
    }
    
    protected final void startSameIdChainParse() {
        // TODO: remove this method once everything is ported
        readDssHeader();
    }
    
    protected final void endOfSameIdChainData() {
        if (!ddmCollectionLenStack.isEmpty()) {
            throw new IllegalStateException("SQLState.NET_COLLECTION_STACK_NOT_EMPTY");
//            agent_.accumulateChainBreakingReadExceptionAndThrow(
//                new DisconnectException(agent_, 
//                new ClientMessageId(SQLState.NET_COLLECTION_STACK_NOT_EMPTY)));
        }
        if (this.dssLength_ != 0) {
            throw new IllegalStateException("SQLState.NET_DSS_NOT_ZERO " + dssLength_);
//            agent_.accumulateChainBreakingReadExceptionAndThrow(
//                new DisconnectException(agent_, 
//                new ClientMessageId(SQLState.NET_DSS_NOT_ZERO)));
        }
        if (dssIsChainedWithSameID_ == true) {
            throw new IllegalStateException("SQLState.NET_DSS_CHAINED_WITH_SAME_ID");
//            agent_.accumulateChainBreakingReadExceptionAndThrow(
//                new DisconnectException(agent_, 
//                new ClientMessageId(SQLState.NET_DSS_CHAINED_WITH_SAME_ID)));
        }
    }



    private void readDssHeader() {
        int correlationID;
        int nextCorrelationID;
        // ensureALayerDataInBuffer(6); TODO @AGG do we need to ensure data is readable?

        // read out the dss length
        dssLength_ = buffer.readShort();

        // Remember the old dss length for decryption only.
        int oldDssLength = dssLength_;

        // check for the continuation bit and update length as needed.
        if ((dssLength_ & 0x8000) == 0x8000) {
            dssLength_ = 32767;
            dssIsContinued_ = true;
        } else {
            dssIsContinued_ = false;
        }

        if (dssLength_ < 6) {
            throw new UnsupportedOperationException();
            // doSyntaxrmSemantics(CodePoint.SYNERRCD_DSS_LESS_THAN_6);
        }

        // If the GDS id is not valid, or
        // if the reply is not an RPYDSS nor
        // a OBJDSS, then throw an exception.
        byte magic = buffer.readByte();
        if (magic != (byte) 0xd0) {
            throw new IllegalStateException(String.format("Magic bit needs to be 0xD0 but was %02x", magic));
            // doSyntaxrmSemantics(CodePoint.SYNERRCD_CBYTE_NOT_D0);
        }

        int gdsFormatter = buffer.readByte() & 0xFF;
        if (((gdsFormatter & 0x02) != 0x02) && ((gdsFormatter & 0x03) != 0x03) && ((gdsFormatter & 0x04) != 0x04)) {
            throw new IllegalStateException("CodePoint.SYNERRCD_FBYTE_NOT_SUPPORTED");
            // doSyntaxrmSemantics(CodePoint.SYNERRCD_FBYTE_NOT_SUPPORTED);
        }

        // Determine if the current DSS is chained with the
        // next DSS, with the same or different request ID.
        if ((gdsFormatter & 0x40) == 0x40) { // on indicates structure chained to next structure
            if ((gdsFormatter & 0x10) == 0x10) {
                dssIsChainedWithSameID_ = true;
                nextCorrelationID = dssCorrelationID_;
            } else {
                dssIsChainedWithSameID_ = false;
                nextCorrelationID = dssCorrelationID_ + 1;
            }
        } else {
            // chaining bit not b'1', make sure DSSFMT bit3 not b'1'
            if ((gdsFormatter & 0x10) == 0x10) { // Next DSS can not have same correlator
                throw new IllegalStateException("CodePoint.SYNERRCD_CHAIN_OFF_SAME_NEXT_CORRELATOR");
                // doSyntaxrmSemantics(CodePoint.SYNERRCD_CHAIN_OFF_SAME_NEXT_CORRELATOR);
            }

            // chaining bit not b'1', make sure no error continuation
            if ((gdsFormatter & 0x20) == 0x20) { // must be 'do not continue on error'
                throw new IllegalStateException("CodePoint.SYNERRCD_CHAIN_OFF_ERROR_CONTINUE");
                // doSyntaxrmSemantics(CodePoint.SYNERRCD_CHAIN_OFF_ERROR_CONTINUE);
            }

            dssIsChainedWithSameID_ = false;
            nextCorrelationID = 1;
        }

        correlationID = buffer.readShort();

        // corrid must be the one expected or a -1 which gets returned in some error
        // cases.
        if ((correlationID != dssCorrelationID_) && (correlationID != 0xFFFF)) {
            // doSyntaxrmSemantics(CodePoint.SYNERRCD_INVALID_CORRELATOR);
            throw new IllegalStateException(
                    "Invalid correlator ID. Got " + correlationID + " expected " + dssCorrelationID_);
        } else {
            dssCorrelationID_ = nextCorrelationID;
        }
        dssLength_ -= 6;
        // if ((gdsFormatter & 0x04) == 0x04) {
        // decryptData(gdsFormatter, oldDssLength); //we have to decrypt data here
        // because
        // }
        // we need the decrypted codepoint. If
        // Data is very long > 32767, we have to
        // get all the data first because decrypt
        // piece by piece doesn't work.
    }
    
    final String readString() {
        int len = ddmScalarLen_;
        ensureBLayerDataInBuffer(len);
        adjustLengths(len);
        String result = buffer.readCharSequence(len, ccsidManager.getCCSID()).toString();
//        String result = currentCCSID.decode(buffer); 
//                netAgent_.getCurrentCcsidManager()
//                            .convertToJavaString(buffer_, pos_, len);
//        pos_ += len;
        return result;
    }
    
    final String readString(int length, Charset encoding) {
        ensureBLayerDataInBuffer(length);
        adjustLengths(length);
        String s = buffer.readCharSequence(length, encoding).toString();
        //String s = new String(buffer.array(), pos_, length, encoding);
        //pos_ += length;
        return s;
    }
    
    final short readShort() {
        // should we be checking dss lengths and ddmScalarLengths here
        ensureBLayerDataInBuffer(2);
        adjustLengths(2);
//        short s = SignedBinary.getShort(buffer_, pos_);

        //pos_ += 2;

        return buffer.readShort();
    }

    boolean checkAndGetReceivedFlag(boolean receivedFlag){
        if (receivedFlag) {
            // this method will throw a disconnect exception if
            // the received flag is already true;
            doSyntaxrmSemantics(CodePoint.SYNERRCD_DUP_OBJ_PRESENT);
        }
        return true;
    }
    
    final void doSyntaxrmSemantics(int syntaxErrorCode) {
        throw new IllegalStateException("SQLState.DRDA_CONNECTION_TERMINATED CONN_DRDA_DATASTREAM_SYNTAX_ERROR " + syntaxErrorCode);
//        DisconnectException e = new DisconnectException(agent_,
//                new ClientMessageId(SQLState.DRDA_CONNECTION_TERMINATED),
//                SqlException.getMessageUtil().getTextMessage(
//                    MessageId.CONN_DRDA_DATASTREAM_SYNTAX_ERROR,
//                    syntaxErrorCode));
            
        // if we are communicating to an older server, we may get a SYNTAXRM on
        // ACCSEC (missing codepoint RDBNAM) if we were unable to convert to
        // EBCDIC (See DERBY-4008/DERBY-4004).  In that case we should chain 
        // the original conversion exception, so it is clear to the user what
        // the problem was.
//        if (netAgent_.exceptionConvertingRdbnam != null) {
//            e.setNextException(netAgent_.exceptionConvertingRdbnam);
//            netAgent_.exceptionConvertingRdbnam = null;
//        }
//        agent_.accumulateChainBreakingReadExceptionAndThrow(e);
    }

    protected final int peekCodePoint() {
        if (!ddmCollectionLenStack.isEmpty()) {
            if (ddmCollectionLenStack.peek() == 0) {
                return END_OF_COLLECTION;
            } else if (ddmCollectionLenStack.peek() < 4) {
                // error
                throw new IllegalStateException("Invalid ddm collection length: " + ddmCollectionLenStack.peek());
            }
        }

        // if there is no more data in the current dss, and the dss is not
        // continued, indicate the end of the same Id chain or read the next dss header.
        if ((dssLength_ == 0) && (!dssIsContinued_)) {
            if (!dssIsChainedWithSameID_) {
                return END_OF_SAME_ID_CHAIN;
            }
            readDssHeader();
        }

        // if (longBufferForDecryption_ == null) //we don't need to do this if it's data
        // stream encryption
        // {
        // ensureBLayerDataInBuffer(4);
        // }
        peekedLength_ = buffer.getShort(buffer.readerIndex()); //buffer.readShort();// ((buffer_[pos_] & 0xff) << 8) + ((buffer_[pos_ + 1] & 0xff) << 0);
        peekedCodePoint_ = buffer.getShort(buffer.readerIndex() + 2); //buffer.readShort(); // ((buffer_[pos_ + 2] & 0xff) << 8) + ((buffer_[pos_ + 3] & 0xff) << 0);

        // check for extended length
        if ((peekedLength_ & 0x8000) == 0x8000) {
            peekExtendedLength();
        } else {
            peekedNumOfExtendedLenBytes_ = 0;
        }
        return peekedCodePoint_;
    }
    
    protected final void pushLengthOnCollectionStack() {
        ddmCollectionLenStack.push(ddmScalarLen_);
        ddmScalarLen_ = 0;
//        System.out.println("@AGG pushed length: " + ddmCollectionLenStack);
    }
    
    protected final void parseLengthAndMatchCodePoint(int expectedCodePoint) {
        int actualCodePoint = 0;
        if (peekedCodePoint_ == END_OF_COLLECTION) {
            actualCodePoint = readLengthAndCodePoint();
        } else {
            actualCodePoint = peekedCodePoint_;
            //pos_ += (4 + peekedNumOfExtendedLenBytes_);
            buffer.readerIndex(buffer.readerIndex() + (4 + peekedNumOfExtendedLenBytes_));
            ddmScalarLen_ = peekedLength_;
            if (peekedNumOfExtendedLenBytes_ == 0 && ddmScalarLen_ != -1) {
                adjustLengths(4);
            } else {
                adjustCollectionAndDssLengths(4 + peekedNumOfExtendedLenBytes_);
            }
            peekedLength_ = 0;
            peekedCodePoint_ = END_OF_COLLECTION;
            peekedNumOfExtendedLenBytes_ = 0;
        }

        if (actualCodePoint != expectedCodePoint) {
            throw new IllegalStateException("Expected code point " + Integer.toHexString(expectedCodePoint)
                    + " but was " + Integer.toHexString(actualCodePoint));
        }
    }

    private int readLengthAndCodePoint() {
        if (!ddmCollectionLenStack.isEmpty()) {
            if (ddmCollectionLenStack.peek() == 0) {
                return END_OF_COLLECTION;
            } else if (ddmCollectionLenStack.peek() < 4) {
                // error
                throw new IllegalStateException("Invalid ddm collection length: " + ddmCollectionLenStack.peek());
            }
        }

        // if there is no more data in the current dss, and the dss is not
        // continued, indicate the end of the same Id chain or read the next dss header.
        if ((dssLength_ == 0) && (!dssIsContinued_)) {
            if (!dssIsChainedWithSameID_) {
                return END_OF_SAME_ID_CHAIN;
            }
            readDssHeader();
        }

        ensureBLayerDataInBuffer(4);
        ddmScalarLen_ = buffer.readShort();
//                ((buffer_[pos_++] & 0xff) << 8) +
//                ((buffer_[pos_++] & 0xff) << 0);
        int codePoint = buffer.readShort();
//                ((buffer_[pos_++] & 0xff) << 8) +
//                ((buffer_[pos_++] & 0xff) << 0);
        adjustLengths(4);

        // check for extended length
        if ((ddmScalarLen_ & 0x8000) == 0x8000) {
            readExtendedLength();
        }
        return codePoint;
    }

    private void readExtendedLength() {
        int numberOfExtendedLenBytes = (ddmScalarLen_ - 0x8000); // fix scroll problem was - 4
        int adjustSize = 0;
        switch (numberOfExtendedLenBytes) {
        case 4:
            ensureBLayerDataInBuffer(4);
            ddmScalarLen_ = buffer.readInt();
//                    ((buffer_[pos_++] & 0xff) << 24) +
//                    ((buffer_[pos_++] & 0xff) << 16) +
//                    ((buffer_[pos_++] & 0xff) << 8) +
//                    ((buffer_[pos_++] & 0xff) << 0);
            adjustSize = 4;
            break;
        case 0:
            ddmScalarLen_ = -1;
            adjustSize = 0;
            break;
        default:
            throw new IllegalStateException("CodePoint.SYNERRCD_INCORRECT_EXTENDED_LEN");
            //doSyntaxrmSemantics(CodePoint.SYNERRCD_INCORRECT_EXTENDED_LEN);
        }

        adjustCollectionAndDssLengths(adjustSize);
    }

    private void adjustCollectionAndDssLengths(int length) {
        // adjust the lengths here.  this is a special case since the
        // extended length bytes do not include their own length.
        Deque<Integer> original = ddmCollectionLenStack;
        ddmCollectionLenStack = new ArrayDeque<>(original.size());
        while (!original.isEmpty()) {
            ddmCollectionLenStack.add(original.pop() - length);
        }
        dssLength_ -= length;
//        System.out.println("@AGG reduced len by " + length + " stack is now: " + ddmCollectionLenStack);
    }
    
    protected final void adjustLengths(int length) {
        ddmScalarLen_ -= length;
        adjustCollectionAndDssLengths(length);
    }

    protected int adjustDdmLength(int ddmLength, int length) {
        ddmLength -= length;
        if (ddmLength == 0) {
            adjustLengths(getDdmLength());
        }
        return ddmLength;
    }
    
    final int getDdmLength() {
        return ddmScalarLen_;
    }

    private void peekExtendedLength() {
        peekedNumOfExtendedLenBytes_ = (peekedLength_ - 0x8004);
        switch (peekedNumOfExtendedLenBytes_) {
        case 4:
            // L L C P Extended Length
            // -->2-bytes<-- --->4-bytes<---
            // We are only peeking the length here, the actual pos_ is still before LLCP. We
            // ensured
            // 4-bytes in peedCodePoint() for the LLCP, and we need to ensure 4-bytes(of
            // LLCP) + the
            // extended length bytes here.
            // if (longBufferForDecryption_ == null) //we ddon't need to do this if it's
            // data stream encryption
            // {
            // ensureBLayerDataInBuffer(4 + 4);
            // }
            // The ddmScalarLen_ we peek here does not include the LLCP and the extended
            // length bytes
            // themselves. So we will add those back to the ddmScalarLen_ so it can be
            // adjusted
            // correctly in parseLengthAndMatchCodePoint(). (since the adjustLengths()
            // method will
            // subtract the length from ddmScalarLen_)
            peekedLength_ = buffer.getInt(4);
            // ((buffer_[pos_ + 4] & 0xff) << 24) +
            // ((buffer_[pos_ + 5] & 0xff) << 16) +
            // ((buffer_[pos_ + 6] & 0xff) << 8) +
            // ((buffer_[pos_ + 7] & 0xff) << 0);
            break;
        case 0:
            peekedLength_ = -1; // this ddm is streamed, so set -1 -> length unknown
            break;
        default:
            throw new IllegalStateException("CodePoint.SYNERRCD_INCORRECT_EXTENDED_LEN");
            // doSyntaxrmSemantics(CodePoint.SYNERRCD_INCORRECT_EXTENDED_LEN);
        }
    }
    
    final int[] readUnsignedShortList() {
        int len = ddmScalarLen_;
        ensureBLayerDataInBuffer(len);
        adjustLengths(len);

        int count = len / 2;
        int[] list = new int[count];

        for (int i = 0; i < count; i++) {
            list[i] = buffer.readUnsignedShort();
//            list[i] = ((buffer_[pos_++] & 0xff) << 8) +
//                    ((buffer_[pos_++] & 0xff) << 0);
        }

        return list;
    }
    
    final int readUnsignedByte() {
        ensureBLayerDataInBuffer(1);
        adjustLengths(1);
        return buffer.readUnsignedByte();
//        return (buffer_[pos_++] & 0xff);
    }

    final byte readByte() {
        ensureBLayerDataInBuffer(1);
        adjustLengths(1);
        return buffer.readByte();
//        return (byte) (buffer_[pos_++] & 0xff);
    }

    
    final byte[] readBytes(int length) {
        ensureBLayerDataInBuffer(length);
        adjustLengths(length);

        byte[] b = new byte[length];
        //System.arraycopy(buffer, pos_, b, 0, length);
        buffer.readBytes(b, 0, length);
        //pos_ += length;
        return b;
    }

    final byte[] readBytes() {
        int len = ddmScalarLen_;
        ensureBLayerDataInBuffer(len);
        adjustLengths(len);

        byte[] b = new byte[len];
        buffer.readBytes(b, 0, len);
//        System.arraycopy(buffer, pos_, b, 0, len);
        //pos_ += len;
        return b;
    }
    
    final int readUnsignedShort() {
        // should we be checking dss lengths and ddmScalarLengths here
        // if yes, i am not sure this is the correct place if we should be checking
        ensureBLayerDataInBuffer(2);
        adjustLengths(2);
        return buffer.readUnsignedShort();
//        return ((buffer_[pos_++] & 0xff) << 8) +
//                ((buffer_[pos_++] & 0xff) << 0);
    }
    
    // this is duplicated in parseColumnMetaData, but different
    // DAGroup under NETColumnMetaData requires a lot more stuffs including
    // precsion, scale and other stuffs
    String parseFastVCS() {
        // doublecheck what readString() does if the length is 0
        return readFastString(readFastUnsignedShort(), ccsidManager.getCCSID());
//                netAgent_.targetTypdef_.getCcsidSbcEncoding());
    }

    final void skipBytes(int length) {
        ensureBLayerDataInBuffer(length);
        adjustLengths(length);
        buffer.skipBytes(length);
        //pos_ += length;
    }

    final void skipBytes() {
        int len = ddmScalarLen_;
        ensureBLayerDataInBuffer(len);
        adjustLengths(len);
        buffer.skipBytes(len);
        //pos_ += len;
    }
    
    final void skipFastBytes(int length) {
        buffer.skipBytes(length);
        //pos_ += length;
    }
    
    protected final void popCollectionStack() {
        // TODO: remove this after done porting
        ddmCollectionLenStack.pop();
    }
    
    final String readFastString(int length, Charset encoding) {
//        String s = new String(buffer_, pos_, length, encoding);
        //pos_ += length;
        return buffer.readCharSequence(length, encoding).toString();
    }
    
    final void readFastIntArray(int[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = buffer.readInt(); //SignedBinary.getInt(buffer_, pos_);
            //pos_ += 4;
        }
    }
    
    final int readFastUnsignedByte() {
        //pos_++;
        return buffer.readUnsignedByte();
//        return (buffer_[pos_++] & 0xff);
    }

    final short readFastShort() {
        //pos_ += 2;
        return buffer.readShort();
//        short s = SignedBinary.getShort(buffer_, pos_);
//        return s;
    }
    
    final long readFastLong() {
//        long l = SignedBinary.getLong(buffer_, pos_);
        //pos_ += 8;
        return buffer.readLong();
    }

    final int readFastUnsignedShort() {
        //pos_ += 2;
        return buffer.readUnsignedShort();
//        return ((buffer_[pos_++] & 0xff) << 8) +
//                ((buffer_[pos_++] & 0xff) << 0);
    }

    final int readFastInt() {
//        int i = SignedBinary.getInt(buffer_, pos_);
        //pos_ += 4;
        return buffer.readInt();
    }

    final String readFastString(int length) {
        String result = buffer.readCharSequence(length, ccsidManager.getCCSID()).toString();
//                            .convertToJavaString(buffer_, pos_, length);
        //pos_ += length;
        return result;
    }

    final byte[] readFastBytes(int length) {
        byte[] b = new byte[length];
        //System.arraycopy(buffer, pos_, b, 0, length);
        buffer.readBytes(b, 0, length);
        //pos_ += length;
        return b;
    }
    
    final byte[] readFastLDBytes() {
        //buffer.skipBytes(1); // @AGG skipping 1 byte based on pos++
        int len = buffer.readShort();
//        int len = ((buffer_[pos_++] & 0xff) << 8) + ((buffer_[pos_++] & 0xff) << 0);
        if (len == 0) {
            return null;
        }

        byte[] b = new byte[len];
        //System.arraycopy(buffer, pos_, b, 0, len);
        buffer.readBytes(b, 0, len);
        //pos_ += len;
        return b;
    }
    
    void ensureBLayerDataInBuffer(int desiredDataSize) {
        // TODO: remove this after done porting
        ensureALayerDataInBuffer(desiredDataSize);
    }

    // Make sure a certain amount of Layer A data is in the buffer.
    // The data will be in the buffer after this method is called.
    void ensureALayerDataInBuffer(int desiredDataSize) {
        if (buffer.readableBytes() < desiredDataSize) {
            throw new IllegalStateException(
                    "Needed to have " + desiredDataSize + " in buffer but only had " + buffer.readableBytes());
        }
    }

}

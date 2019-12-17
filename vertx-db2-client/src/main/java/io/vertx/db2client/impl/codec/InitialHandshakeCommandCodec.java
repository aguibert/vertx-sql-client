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

import static io.vertx.db2client.impl.codec.CapabilitiesFlag.CLIENT_CONNECT_ATTRS;
import static io.vertx.db2client.impl.codec.CapabilitiesFlag.CLIENT_CONNECT_WITH_DB;
import static io.vertx.db2client.impl.codec.CapabilitiesFlag.CLIENT_DEPRECATE_EOF;
import static io.vertx.db2client.impl.codec.CapabilitiesFlag.CLIENT_PLUGIN_AUTH;
import static io.vertx.db2client.impl.codec.CapabilitiesFlag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
import static io.vertx.db2client.impl.codec.CapabilitiesFlag.CLIENT_SECURE_CONNECTION;
import static io.vertx.db2client.impl.codec.CapabilitiesFlag.CLIENT_SSL;
import static io.vertx.db2client.impl.codec.Packets.ERROR_PACKET_HEADER;
import static io.vertx.db2client.impl.codec.Packets.OK_PACKET_HEADER;
import static io.vertx.db2client.impl.codec.Packets.PACKET_PAYLOAD_LENGTH_LIMIT;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.vertx.db2client.impl.command.InitialHandshakeCommand;
import io.vertx.db2client.impl.drda.CCSIDManager;
import io.vertx.db2client.impl.drda.CodePoint;
import io.vertx.db2client.impl.drda.DRDAConnectRequest;
import io.vertx.db2client.impl.drda.DRDAConnectResponse;
import io.vertx.db2client.impl.drda.DRDAConnectResponse.RDBAccessData;
import io.vertx.db2client.impl.drda.DRDAConstants;
import io.vertx.db2client.impl.util.BufferUtils;
import io.vertx.db2client.impl.util.CachingSha2Authenticator;
import io.vertx.db2client.impl.util.Native41Authenticator;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.command.CommandResponse;

class InitialHandshakeCommandCodec extends AuthenticationCommandBaseCodec<Connection, InitialHandshakeCommand> {

    private static final int AUTH_PLUGIN_DATA_PART1_LENGTH = 8;

    private static final int ST_CONNECTING = 0;
    private static final int ST_AUTHENTICATING = 1;
    private static final int ST_CONNECTED = 2;
    
    private static final int TARGET_SECURITY_MEASURE = DRDAConstants.SECMEC_USRIDPWD;//0x03;//0x0A;
    
    // TODO: May need to move this to a higher scope?
    private final CCSIDManager ccsidManager = new CCSIDManager();
    
    // TODO: @AGG may need to move this to connection level
    // Correlation Token of the source sent to the server in the accrdb.
    // It is saved like the prddta in case it is needed for a connect reflow.
    private byte[] crrtkn_;
    
    // TODO: @AGG move to conn lvl
    // Product-Specific Data (prddta) sent to the server in the accrdb command.
    // The prddta has a specified format.  It is saved in case it is needed again
    // since it takes a little effort to compute.  Saving this information is
    // useful for when the connect flows need to be resent (right now the connect
    // flow is resent when this driver disconnects and reconnects with
    // non unicode ccsids.  this is done when the server doesn't recoginze the
    // unicode ccsids).
    //
    private byte[] prddta_;
    private String productID_;
    private int svrcod_ = CodePoint.SVRCOD_INFO;

    private int status = ST_CONNECTING;

    InitialHandshakeCommandCodec(InitialHandshakeCommand cmd) {
        super(cmd);
    }

    @Override
    void encode(DB2Encoder encoder) {
        super.encode(encoder);
        sendInitialHandshake();
    }

    @Override
    void decodePayload(ByteBuf payload, int payloadLength) {
        DRDAConnectResponse response = new DRDAConnectResponse(payload, ccsidManager);
        try {
            switch (status) {
            case ST_CONNECTING:
                //handleInitialHandshake(payload);
                response.readExchangeServerAttributes();
                response.readAccessSecurity(TARGET_SECURITY_MEASURE);
                status = ST_AUTHENTICATING;
                ByteBuf packet = allocateBuffer();
                int packetStartIdx = packet.writerIndex();
                DRDAConnectRequest securityCheck = new DRDAConnectRequest(packet, ccsidManager);
                securityCheck.buildSECCHK(TARGET_SECURITY_MEASURE,
                        cmd.database(),
                        cmd.username(),
                        cmd.password(),
                        null, //sectkn, 
                        null); //sectkn2
                securityCheck.buildACCRDB(cmd.database(), 
                        false, //readOnly, 
                        getCorrelationToken(), 
                        getProductData(),
                        DRDAConstants.SYSTEM_ASC);
                // build ACCRDB
                securityCheck.completeCommand();
                int lenOfPayload = packet.writerIndex() - packetStartIdx;
                sendPacket(packet, lenOfPayload);
                return;
            case ST_AUTHENTICATING:
                //handleAuthentication(payload);
                response.readSecurityCheck();
                RDBAccessData accData = response.readAccessDatabase();
                if (accData.correlationToken != null)
                    crrtkn_ = accData.correlationToken;
                setSvrcod(accData.svrcod);
                completionHandler.handle(CommandResponse.success(cmd.connection()));
                return;
            default: 
                throw new IllegalStateException("Unknown state: " + status);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            completionHandler.handle(CommandResponse.failure(t));
        }
    }
    
    void setSvrcod(int svrcod) {
        if (svrcod > svrcod_) {
            svrcod_ = svrcod;
        }
    }
    
    private void sendInitialHandshake() {
        ByteBuf packet = allocateBuffer();
        // encode packet header
        int packetStartIdx = packet.writerIndex();
        DRDAConnectRequest cmd = new DRDAConnectRequest(packet, ccsidManager);
        try {
            cmd.buildEXCSAT(DRDAConstants.EXTNAM, // externalName,
                    0x0A, // targetAgent,
                    DRDAConstants.TARGET_SQL_AM, // targetSqlam,
                    0x0C, // targetRdb,
                    TARGET_SECURITY_MEASURE, // 0x07, //targetSecmgr,
                    0, // 0x05, // targetCmntcpip,
                    0, // targetCmnappc, (not used)
                    0, // 0x07, //targetXamgr,
                    0, // targetSyncptmgr,
                    0, // targetRsyncmgr,
                    CCSIDManager.TARGET_UNICODE_MGR // targetUnicodemgr
            );
            cmd.buildACCSEC(0x03, this.cmd.database(), null);
            cmd.completeCommand();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // set payload length
        int lenOfPayload = packet.writerIndex() - packetStartIdx;
        // packet.setMediumLE(packetStartIdx, lenOfPayload);
        sendPacket(packet, lenOfPayload);
    }

    private void handleInitialHandshake(ByteBuf payload) {
        short protocolVersion = payload.readUnsignedByte();

        String serverVersion = BufferUtils.readNullTerminatedString(payload, StandardCharsets.US_ASCII);
        // we assume the server version follows ${major}.${minor}.${release} in
        // https://dev.mysql.com/doc/refman/8.0/en/which-version.html
        String[] versionNumbers = serverVersion.split("\\.");
        int majorVersion = Integer.parseInt(versionNumbers[0]);
        int minorVersion = Integer.parseInt(versionNumbers[1]);
        // we should truncate the possible suffixes here
        String releaseVersion = versionNumbers[2];
        int releaseNumber;
        int indexOfFirstSeparator = releaseVersion.indexOf("-");
        if (indexOfFirstSeparator != -1) {
            // handle unstable release suffixes
            String releaseNumberString = releaseVersion.substring(0, indexOfFirstSeparator);
            releaseNumber = Integer.parseInt(releaseNumberString);
        } else {
            releaseNumber = Integer.parseInt(versionNumbers[2]);
        }
        if (majorVersion == 5 && (minorVersion < 7 || (minorVersion == 7 && releaseNumber < 5))) {
            // EOF_HEADER is enabled
        } else {
            encoder.clientCapabilitiesFlag |= CLIENT_DEPRECATE_EOF;
        }

        long connectionId = payload.readUnsignedIntLE();

        // read first part of scramble
        this.authPluginData = new byte[NONCE_LENGTH];
        payload.readBytes(authPluginData, 0, AUTH_PLUGIN_DATA_PART1_LENGTH);

        // filler
        payload.readByte();

        // read lower 2 bytes of Capabilities flags
        int lowerServerCapabilitiesFlags = payload.readUnsignedShortLE();

        short characterSet = payload.readUnsignedByte();

        int statusFlags = payload.readUnsignedShortLE();

        // read upper 2 bytes of Capabilities flags
        int capabilityFlagsUpper = payload.readUnsignedShortLE();
        final int serverCapabilitiesFlags = (lowerServerCapabilitiesFlags | (capabilityFlagsUpper << 16));

        // length of the combined auth_plugin_data (scramble)
        short lenOfAuthPluginData;
        boolean isClientPluginAuthSupported = (serverCapabilitiesFlags & CapabilitiesFlag.CLIENT_PLUGIN_AUTH) != 0;
        if (isClientPluginAuthSupported) {
            lenOfAuthPluginData = payload.readUnsignedByte();
        } else {
            payload.readerIndex(payload.readerIndex() + 1);
            lenOfAuthPluginData = 0;
        }

        // 10 bytes reserved
        payload.readerIndex(payload.readerIndex() + 10);

        // Rest of the plugin provided data
        payload.readBytes(authPluginData, AUTH_PLUGIN_DATA_PART1_LENGTH,
                Math.max(NONCE_LENGTH - AUTH_PLUGIN_DATA_PART1_LENGTH, lenOfAuthPluginData - 9));
        payload.readByte(); // reserved byte

        // we assume the server supports auth plugin
        final String authPluginName = BufferUtils.readNullTerminatedString(payload, StandardCharsets.UTF_8);

        boolean upgradeToSsl = false;
        // SslMode sslMode = cmd.sslMode();
        // switch (sslMode) {
        // case DISABLED:
        // upgradeToSsl = false;
        // break;
        // case PREFERRED:
        // upgradeToSsl = isTlsSupportedByServer(serverCapabilitiesFlags);
        // break;
        // case REQUIRED:
        // case VERIFY_CA:
        // case VERIFY_IDENTITY:
        // upgradeToSsl = true;
        // break;
        // default:
        // completionHandler.handle(CommandResponse.failure(new
        // IllegalStateException("Unknown SSL mode to handle: " + sslMode)));
        // return;
        // }

        // if (upgradeToSsl) {
        // encoder.clientCapabilitiesFlag |= CLIENT_SSL;
        // sendSslRequest();
        //
        // encoder.socketConnection.upgradeToSSLConnection(upgrade -> {
        // if (upgrade.succeeded()) {
        // doSendHandshakeResponseMessage(authPluginName, authPluginData,
        // serverCapabilitiesFlags);
        // } else {
        // completionHandler.handle(CommandResponse.failure(upgrade.cause()));
        // }
        // });
        // } else {
        doSendHandshakeResponseMessage(authPluginName, authPluginData, serverCapabilitiesFlags);
        // }
    }

    // private void setAdditionalClientCapabilitiesFlags() {
    // if (!cmd.useAffectedRows()) {
    // encoder.clientCapabilitiesFlag |= CLIENT_FOUND_ROWS;
    // }
    // }

    private void doSendHandshakeResponseMessage(String authMethodName, byte[] nonce, int serverCapabilitiesFlags) {
        if (!cmd.database().isEmpty()) {
            encoder.clientCapabilitiesFlag |= CLIENT_CONNECT_WITH_DB;
        }
        Map<String, String> clientConnectionAttributes = cmd.connectionAttributes();
        if (clientConnectionAttributes != null && !clientConnectionAttributes.isEmpty()) {
            encoder.clientCapabilitiesFlag |= CLIENT_CONNECT_ATTRS;
        }
        encoder.clientCapabilitiesFlag &= serverCapabilitiesFlags;
        sendHandshakeResponseMessage(cmd.username(), cmd.password(), cmd.database(), nonce, authMethodName,
                clientConnectionAttributes);
    }

    private void handleAuthentication(ByteBuf payload) {
        int header = payload.getUnsignedByte(payload.readerIndex());
        switch (header) {
        case OK_PACKET_HEADER:
            status = ST_CONNECTED;
            completionHandler.handle(CommandResponse.success(cmd.connection()));
            break;
        case ERROR_PACKET_HEADER:
            handleErrorPacketPayload(payload);
            break;
        case AUTH_SWITCH_REQUEST_STATUS_FLAG:
            handleAuthSwitchRequest(cmd.password().getBytes(), payload);
            break;
        // case AUTH_MORE_DATA_STATUS_FLAG:
        // handleAuthMoreData(cmd.password().getBytes(), payload);
        // break;
        default:
            completionHandler.handle(
                    CommandResponse.failure(new IllegalStateException("Unhandled state with header: " + header)));
        }
    }

    private void handleAuthSwitchRequest(byte[] password, ByteBuf payload) {
        // Protocol::AuthSwitchRequest
        payload.skipBytes(1); // status flag, always 0xFE
        String pluginName = BufferUtils.readNullTerminatedString(payload, StandardCharsets.UTF_8);
        byte[] nonce = new byte[NONCE_LENGTH];
        payload.readBytes(nonce);
        byte[] scrambledPassword;
        switch (pluginName) {
        case "mysql_native_password":
            scrambledPassword = Native41Authenticator.encode(password, nonce);
            break;
        case "caching_sha2_password":
            scrambledPassword = CachingSha2Authenticator.encode(password, nonce);
            break;
        default:
            completionHandler.handle(CommandResponse
                    .failure(new UnsupportedOperationException("Unsupported authentication method: " + pluginName)));
            return;
        }
        sendBytesAsPacket(scrambledPassword);
    }

    private void sendSslRequest() {
        ByteBuf packet = allocateBuffer(36);
        // encode packet header
        packet.writeMediumLE(32);
        packet.writeByte(sequenceId);

        // encode SSLRequest payload
        packet.writeIntLE(encoder.clientCapabilitiesFlag);
        packet.writeIntLE(PACKET_PAYLOAD_LENGTH_LIMIT);
        packet.writeZero(23); // filler

        sendNonSplitPacket(packet);
    }

    private void sendHandshakeResponseMessage(String username, String password, String database, byte[] nonce,
            String authMethodName, Map<String, String> clientConnectionAttributes) {
        ByteBuf packet = allocateBuffer();
        // encode packet header
        int packetStartIdx = packet.writerIndex();
        packet.writeMediumLE(0); // will set payload length later by calculation
        packet.writeByte(sequenceId);

        // encode packet payload
        int clientCapabilitiesFlags = encoder.clientCapabilitiesFlag;
        packet.writeIntLE(clientCapabilitiesFlags);
        packet.writeIntLE(PACKET_PAYLOAD_LENGTH_LIMIT);
        packet.writeZero(23); // filler
        BufferUtils.writeNullTerminatedString(packet, username, StandardCharsets.UTF_8);
        if (password.isEmpty()) {
            packet.writeByte(0);
        } else {
            byte[] scrambledPassword;
            switch (authMethodName) {
            case "mysql_native_password":
                scrambledPassword = Native41Authenticator.encode(password.getBytes(), nonce);
                break;
            case "caching_sha2_password":
                scrambledPassword = CachingSha2Authenticator.encode(password.getBytes(), nonce);
                break;
            default:
                completionHandler.handle(CommandResponse.failure(
                        new UnsupportedOperationException("Unsupported authentication method: " + authMethodName)));
                return;
            }
            if ((clientCapabilitiesFlags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                BufferUtils.writeLengthEncodedInteger(packet, scrambledPassword.length);
                packet.writeBytes(scrambledPassword);
            } else if ((clientCapabilitiesFlags & CLIENT_SECURE_CONNECTION) != 0) {
                packet.writeByte(scrambledPassword.length);
                packet.writeBytes(scrambledPassword);
            } else {
                packet.writeByte(0);
            }
        }
        if ((clientCapabilitiesFlags & CLIENT_CONNECT_WITH_DB) != 0) {
            BufferUtils.writeNullTerminatedString(packet, database, StandardCharsets.UTF_8);
        }
        if ((clientCapabilitiesFlags & CLIENT_PLUGIN_AUTH) != 0) {
            BufferUtils.writeNullTerminatedString(packet, authMethodName, StandardCharsets.UTF_8);
        }
        if ((clientCapabilitiesFlags & CLIENT_CONNECT_ATTRS) != 0) {
            encodeConnectionAttributes(clientConnectionAttributes, packet);
        }

        // set payload length
        int payloadLength = packet.writerIndex() - packetStartIdx - 4;
        packet.setMediumLE(packetStartIdx, payloadLength);

        sendPacket(packet, payloadLength);
    }

    private boolean isTlsSupportedByServer(int serverCapabilitiesFlags) {
        return (serverCapabilitiesFlags & CLIENT_SSL) != 0;
    }
    
    // Construct the correlation token.
    // The crrtkn has the following format.
    //
    // <Almost IP address>.<local port number><current time in millis>
    // |                   | |               ||                  |
    // +----+--------------+ +-----+---------++---------+--------+
    //      |                      |                |
    //    8 bytes               4 bytes         6 bytes
    // Total lengtho of 19 bytes.
    //
    // 1 char for each 1/2 byte in the IP address.
    // If the first character of the <IP address> or <port number>
    // starts with '0' thru '9', it will be mapped to 'G' thru 'P'.
    // Reason for mapping the IP address is in order to use the crrtkn as the LUWID when using SNA in a hop site.
    private byte[] getCorrelationToken() {
        // allocate the crrtkn array.
        if (crrtkn_ != null) {
            return crrtkn_;
        } else {
            crrtkn_ = new byte[19];
        }

        byte[] localAddressBytes;
        try {
            localAddressBytes = Inet4Address.getLocalHost().getAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }

        // IP addresses are returned in a 4 byte array.
        // Obtain the character representation of each half byte.
        for (int i = 0, j = 0; i < 4; i++, j += 2) {

            // since a byte is signed in java, convert any negative
            // numbers to positive before shifting.
            int num = localAddressBytes[i] < 0 ? localAddressBytes[i] + 256 : localAddressBytes[i];
            int halfByte = (num >> 4) & 0x0f;

            // map 0 to G
            // The first digit of the IP address is is replaced by
            // the characters 'G' thro 'P'(in order to use the crrtkn as the LUWID when using
            // SNA in a hop site). For example, 0 is mapped to G, 1 is mapped H,etc.
            if (i == 0) {
                crrtkn_[j] = ccsidManager.getCCSID().encode("" + (char) (halfByte + 'G')).get(); 
                        //ccsidManager.getCCSID().numToSnaRequiredCrrtknChar_[halfByte];
            } else {
                crrtkn_[j] = ccsidManager.getCCSID().encode("" + halfByte).get();
                        //netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];
            }

            halfByte = (num) & 0x0f;
            crrtkn_[j + 1] = ccsidManager.getCCSID().encode("" + halfByte).get();
            //netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];
        }

        // fill the '.' in between the IP address and the port number
        crrtkn_[8] = ccsidManager.getCCSID().encode(".").get();

        // Port numbers have values which fit in 2 unsigned bytes.
        // Java returns port numbers in an int so the value is not negative.
        // Get the character representation by converting the
        // 4 low order half bytes to the character representation.
        int num = encoder.socketConnection.socket().localAddress().port();
        //int num = netAgent_.socket_.getLocalPort();

        int halfByte = (num >> 12) & 0x0f;
        crrtkn_[9] = ccsidManager.getCCSID().encode("" + halfByte).get(); 
                //netAgent_.getCurrentCcsidManager().numToSnaRequiredCrrtknChar_[halfByte];
        halfByte = (num >> 8) & 0x0f;
        crrtkn_[10] = ccsidManager.getCCSID().encode("" + halfByte).get();
                //netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];
        halfByte = (num >> 4) & 0x0f;
        crrtkn_[11] = ccsidManager.getCCSID().encode("" + halfByte).get();
                //netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];
        halfByte = (num) & 0x0f;
        crrtkn_[12] = ccsidManager.getCCSID().encode("" + halfByte).get();
                //netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];

        // The final part of CRRTKN is a 6 byte binary number that makes the
        // crrtkn unique, which is usually the time stamp/process id.
        // If the new time stamp is the
        // same as one of the already created ones, then recreate the time stamp.
        long time = System.currentTimeMillis();

        for (int i = 0; i < 6; i++) {
            // store 6 bytes of 8 byte time into crrtkn
            crrtkn_[i + 13] = (byte) (time >>> (40 - (i * 8)));
        }
        return crrtkn_;
    }
    
    private byte[] getProductData() {
        if (prddta_ != null) {
            return prddta_;
        } 

        ByteBuffer prddta_ = ByteBuffer.allocate(DRDAConstants.PRDDTA_MAXSIZE);

        for (int i = 0; i < DRDAConstants.PRDDTA_ACCT_SUFFIX_LEN_BYTE; i++) {
            prddta_.put(i, ccsidManager.getCCSID().encode(" ").get());
        }

        // Start inserting data right after the length byte.
        prddta_.position(DRDAConstants.PRDDTA_LEN_BYTE + 1);

        prddta_.put(ccsidManager.getCCSID().encode(DRDAConstants.PRDID));//, prddta_);

        prddta_.put(ccsidManager.getCCSID().encode(DRDAConstants.PRDDTA_PLATFORM_ID));
//        success &= ccsidMgr.encode(
//                CharBuffer.wrap(DRDAConstants.PRDDTA_PLATFORM_ID),
//                prddta_, agent_);

        int prddtaLen = prddta_.position();

        String extnamTruncated = DRDAConstants.EXTNAM.substring(0, Math.min(DRDAConstants.EXTNAM.length(), DRDAConstants.PRDDTA_APPL_ID_FIXED_LEN));
        prddta_.put(ccsidManager.getCCSID().encode(extnamTruncated));
//        success &= ccsidMgr.encode(
//                CharBuffer.wrap(extnam_, 0, extnamTruncateLength),
//                prddta_, agent_);

//        if (SanityManager.DEBUG) {
//            // The encode() calls above should all complete without overflow,
//            // since we control the contents of the strings. Verify this in
//            // sane mode so that we notice it if the strings change so that
//            // they go beyond the max size of PRDDTA.
//            SanityManager.ASSERT(success,
//                "PRDID, PRDDTA_PLATFORM_ID and EXTNAM exceeded PRDDTA_MAXSIZE");
//        }

        prddtaLen += DRDAConstants.PRDDTA_APPL_ID_FIXED_LEN;

        prddtaLen += DRDAConstants.PRDDTA_USER_ID_FIXED_LEN;

        // Mark that we have an empty suffix in PRDDTA_ACCT_SUFFIX_LEN_BYTE.
        prddta_.put(DRDAConstants.PRDDTA_ACCT_SUFFIX_LEN_BYTE, (byte) 0);
        prddtaLen++;

        // the length byte value does not include itself.
        prddta_.put(DRDAConstants.PRDDTA_LEN_BYTE, (byte) (prddtaLen - 1));
        
        this.prddta_ = prddta_.array();
        return this.prddta_;
    }

}

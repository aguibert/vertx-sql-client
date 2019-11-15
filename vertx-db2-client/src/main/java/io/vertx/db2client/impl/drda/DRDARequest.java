package io.vertx.db2client.impl.drda;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

import org.apache.derby.client.net.SQLState;

import io.netty.buffer.ByteBuf;

public class DRDARequest {
    
    // TODO
    // Derby Product Identifiers as defined by the Open Group.
    // See http://www.opengroup.org/dbiop/prodid.htm for the
    // list of legal DRDA Product Identifiers.
    //
    public  static  final   String  DERBY_DRDA_SERVER_ID = "CSS";
    public  static  final   String  DERBY_DRDA_CLIENT_ID = "DNC";
    
    private final ByteBuf buffer;
    private final CCSIDManager ccsidManager;
    
    private Deque<Integer> markStack = new ArrayDeque<>(4);

    //  This Object tracks the location of the current
    //  Dss header length bytes.  This is done so
    //  the length bytes can be automatically
    //  updated as information is added to this stream.
    private int dssLengthLocation_ = 0;

    // tracks the request correlation ID to use for commands and command objects.
    // this is automatically updated as commands are built and sent to the server.
    private int correlationID_ = 0;

    private boolean simpleDssFinalize = false;
    
    public DRDARequest(ByteBuf buffer, CCSIDManager ccsidManager) {
        this.buffer = buffer;
        this.ccsidManager = ccsidManager;
    }
    
    // The Access RDB (ACCRDB) command makes a named relational database (RDB)
    // available to a requester by creating an instance of an SQL application
    // manager.  The access RDB command then binds the created instance to the target
    // agent and to the RDB. The RDB remains available (accessed) until
    // the communications conversation is terminate.
    public void buildACCRDB(String rdbnam,
                     boolean readOnly,
                     byte[] crrtkn,
                     byte[] prddta,
                     String typdef) {
        createCommand();

        markLengthBytes(CodePoint.ACCRDB);

        // the relational database name specifies the name of the rdb to
        // be accessed.  this can be different sizes depending on the level of
        // support.  the size will have ben previously checked so at this point just
        // write the data and pad with the correct number of bytes as needed.
        // this instance variable is always required.
        buildRDBNAM(rdbnam,true);

        // the rdb access manager class specifies an instance of the SQLAM
        // that accesses the RDB.  the sqlam manager class codepoint
        // is always used/required for this.  this instance variable
        // is always required.
        buildRDBACCCL();

        // product specific identifier specifies the product release level
        // of this driver.  see the hard coded value in the NetConfiguration class.
        // this instance variable is always required.
        buildPRDID();

        // product specific data.  this is an optional parameter which carries
        // product specific information.  although it is optional, it will be
        // sent to the server.  use the first byte to determine the number
        // of the prddta bytes to write to the buffer. note: this length
        // doesn't include itself so increment by it by 1 to get the actual
        // length of this data.
        buildPRDDTA(prddta);


        // the typdefnam parameter specifies the name of the data type to data representation
        // mappings used when this driver sends command data objects.
        buildTYPDEFNAM(typdef);

//        if (crrtkn == null) {
//            constructCrrtkn();
//        }

        Objects.requireNonNull(crrtkn);
        buildCRRTKN(crrtkn);

        // This specifies the single-byte, double-byte
        // and mixed-byte CCSIDs of the Scalar Data Arrays (SDAs) in the identified
        // data type to the data representation mapping definitions.  This can
        // contain 3 CCSIDs.  The driver will only send the ones which were set.
        buildTYPDEFOVR(true,
                CCSIDManager.TARGET_UNICODE_MGR,
                true,
                CCSIDManager.TARGET_UNICODE_MGR,
                true,
                CCSIDManager.TARGET_UNICODE_MGR);

        // RDB allow update is an optional parameter which indicates
        // whether the RDB allows the requester to perform update operations
        // in the RDB.  If update operations are not allowed, this connection
        // is limited to read-only access of the RDB resources.
        buildRDBALWUPD(readOnly);



        // the Statement Decimal Delimiter (STTDECDEL),
        // Statement String Delimiter (STTSTRDEL),
        // and Target Default Value Return (TRGDFTRT) are all optional
        // instance variables which will not be sent to the server.

        // the command and the dss are complete so make the call to notify
        // the request object.
        updateLengthBytes();
        
        finalizeDssLength(); // @AGG added
    }
    
    public void buildSECCHK(int secmec, String rdbnam, String user, String password, byte[] sectkn, byte[] sectkn2){
        createCommand();
        markLengthBytes(CodePoint.SECCHK);

        // always send the negotiated security mechanism for the connection.
        buildSECMEC(secmec);

        // the rdbnam will be built and sent. different sqlam levels support
        // different lengths. at this point the length has been checked against
        // the maximum allowable length. so write the bytes and padd up to the
        // minimum length if needed.
        buildRDBNAM(rdbnam, false);
        if (user != null) {
            buildUSRID(user);
        }
        if (password != null) {
            buildPASSWORD(password);
        }
        if (sectkn != null) {
            buildSECTKN(sectkn);
        }
        if (sectkn2 != null) {
            buildSECTKN(sectkn2);
        }
        updateLengthBytes();

    }
    
    public void buildACCSEC(int secmec, String rdbnam, byte[] sectkn) throws SQLException {
        createCommand();

        // place the llcp for the ACCSEC in the buffer. save the length bytes for
        // later update
        markLengthBytes(CodePoint.ACCSEC);

        // the security mechanism is a required instance variable. it will
        // always be sent.
        buildSECMEC(secmec);

        // the rdbnam will be built and sent. different sqlam levels support
        // different lengths. at this point the length has been checked against
        // the maximum allowable length. so write the bytes and padd up to the
        // minimum length if needed. We want to defer sending the rdbnam if an
        // EBCDIC conversion is not possible.
        buildRDBNAM(rdbnam, true);

        if (sectkn != null) {
            buildSECTKN(sectkn);
        }

        // the accsec command is complete so notify the the request object to
        // update the ddm length and the dss header length.
        updateLengthBytes();
        
        finalizeDssLength(); // @AGG added
    }
    
    private void buildRDBALWUPD(boolean readOnly) {
        // TODO: @AGG collapse
        if (readOnly) {
            writeScalar1Byte(CodePoint.RDBALWUPD, CodePoint.FALSE);
        }
    }
    
    private void buildTYPDEFOVR(boolean sendCcsidSbc, int ccsidSbc, boolean sendCcsidDbc, int ccsidDbc,
            boolean sendCcsidMbc, int ccsidMbc) {
        // TODO: @AGG collapse
        markLengthBytes(CodePoint.TYPDEFOVR);
        // write the single-byte ccsid used by this driver.
        if (sendCcsidSbc) {
            writeScalar2Bytes(CodePoint.CCSIDSBC, ccsidSbc);
        }

        // write the double-byte ccsid used by this driver.
        if (sendCcsidDbc) {
            writeScalar2Bytes(CodePoint.CCSIDDBC, ccsidDbc);
        }

        // write the mixed-byte ccsid used by this driver
        if (sendCcsidMbc) {
            writeScalar2Bytes(CodePoint.CCSIDMBC, ccsidMbc);
        }

        updateLengthBytes();

    }
    
    private void buildCRRTKN(byte[] crrtkn) {
        // TODO: @AGG collapse
        writeScalarBytes(CodePoint.CRRTKN, crrtkn);
    }
    
    private void buildTYPDEFNAM(String typdefnam) {
        // TODO: @AGG collapse
        writeScalarString(CodePoint.TYPDEFNAM, typdefnam);
    }
    
    private void buildPRDDTA(byte[] prddta) {
        // TODO: @AGG collapse
        int prddtaLength = (prddta[DRDAConstants.PRDDTA_LEN_BYTE] & 0xff) + 1;
        writeScalarBytes(CodePoint.PRDDTA, prddta, 0, prddtaLength);
    }
    
    private void buildPRDID() {
        // TODO: @AGG collapse this
        writeScalarString(CodePoint.PRDID, DRDAConstants.PRDID);  // product id is hard-coded to DNC01000 for dnc 1.0.
    }
    
    private void buildRDBACCCL() {
        // TODO: @AGG collapse this method after porting
        writeScalar2Bytes(CodePoint.RDBACCCL, CodePoint.SQLAM);
    }
    
    private void buildUSRID(String usrid) {
        writeScalarString(CodePoint.USRID, usrid,0,DRDAConstants.USRID_MAXSIZE,
                SQLState.NET_USERID_TOO_LONG);
    }
    
    private void buildPASSWORD(String password) {
        int passwordLength = password.length();
        if ((passwordLength == 0) ) {
            throw new IllegalArgumentException("Password size must be > 0");
        }
        writeScalarString(CodePoint.PASSWORD, password, 0, DRDAConstants.PASSWORD_MAXSIZE,
                SQLState.NET_PASSWORD_TOO_LONG);
    }
    
    private void buildSECTKN(byte[] sectkn) {
        if (sectkn.length > 32763) {
            throw new IllegalArgumentException("SQLState.NET_SECTKN_TOO_LONG");
        }
        writeScalarBytes(CodePoint.SECTKN, sectkn);
    }
    
    /**
     * 
     * Relational Database Name specifies the name of a relational database
     * of the server.
     * if length of RDB name &lt;= 18 characters, there is not change to the format
     * of the RDB name.  The length of the RDBNAM remains fixed at 18 which includes
     * any right bland padding if necessary.
     * if length of the RDB name is &gt; 18 characters, the length of the RDB name is
     * identical to the length of the RDB name.  No right blank padding is required.
     * @param rdbnam  name of the database.
     * @param dontSendOnConversionError omit sending the RDBNAM if there is an
     * exception converting to EBCDIC.  This will be used by ACCSEC to defer
     * sending the RDBNAM to SECCHK if it can't be converted.
     *
     */
    private void buildRDBNAM(String rdbnam, boolean dontSendOnConversionError) {
        //DERBY-4805(Increase the length of the RDBNAM field in the 
        // DRDA implementation)
        //The new RDBNAM length in 10.11 is 1024bytes(it used to be 254 bytes).
        //But if a 10.11 or higher client talks to a 10.10 or under server with
        // a RDBNAM > 254 bytes, it will result in a protocol exception
        // because those servers do not support RDBNAM greater than 254 bytes.
        // This behavior will logged in the jira.
        //One way to fix this would have been to check the server version
        // before hand but we do not have that information when the client is
        // first trying to establish connection to the server by sending the
        // connect request along with the RDBNAM.
        int maxRDBlength = 1024;
        writeScalarString(CodePoint.RDBNAM, rdbnam,
                18, //minimum RDBNAM length in bytes
                maxRDBlength,  
                SQLState.NET_DBNAME_TOO_LONG);
                
    }
    
    // Precondition: valid secmec is assumed.
    private void buildSECMEC(int secmec) {
//        writeScalar2Bytes(CodePoint.SECMEC, secmec);
        ensureLength(6);
        buffer.writeByte((byte) 0x00);
        buffer.writeByte((byte) 0x06);
        buffer.writeShort(CodePoint.SECMEC);
        buffer.writeShort(secmec);
    }
    
    // build the Exchange Server Attributes Command.
    // This command sends the following information to the server.
    // - this driver's server class name
    // - this driver's level of each of the manager's it supports
    // - this driver's product release level
    // - this driver's external name
    // - this driver's server name
    public void buildEXCSAT(String externalName,
                     int targetAgent,
                     int targetSqlam,
                     int targetRdb,
                     int targetSecmgr,
                     int targetCmntcpip,
                     int targetCmnappc,
                     int targetXamgr,
                     int targetSyncptmgr,
                     int targetRsyncmgr,
                     int targetUnicodemgr) throws SQLException {
        createCommand();

        // begin excsat collection by placing the 4 byte llcp in the buffer.
        // the length of this command will be computed later and "filled in"
        // with the call to request.updateLengthBytes().
        markLengthBytes(CodePoint.EXCSAT);

        // place the external name for the client into the buffer.
        // the external name was previously calculated before the call to this method.
        buildEXTNAM(externalName);

        // place the server name for the client into the buffer.
        buildSRVNAM(getHostname());

        // place the server release level for the client into the buffer.
        // this is a hard coded value for the driver.
        buildSRVRLSLV();

        // the managers supported by this driver and their levels will
        // be sent to the server.  the variables which store these values
        // were initialized during object constrcution to the highest values
        // supported by the driver.

        // for the case of the manager levels object, there is no
        // need to have the length of the ddm object dynamically calculated
        // because this method knows exactly how many will be sent and can set
        // this now.
        // each manager level class and level are 4 bytes long and
        // right now 5 are being sent for a total of 20 bytes or 0x14 bytes.
        // writeScalarHeader will be called to insert the llcp.
        buildMGRLVLLS(targetAgent,
                targetSqlam,
                targetRdb,
                targetSecmgr,
                targetXamgr,
                targetSyncptmgr,
                targetRsyncmgr,
                targetUnicodemgr);


        // place the server class name into the buffer.
        // this value is hard coded for the driver.
        buildSRVCLSNM();

        // the excsat command is complete so the updateLengthBytes method
        // is called to dynamically compute the length for this command and insert
        // it into the buffer
        updateLengthBytes();
    }
    
    private void buildSRVCLSNM() throws SQLException {
        // Server class name is hard-coded to QDERBY/JVM for dnc.
        writeScalarString(CodePoint.SRVCLSNM, "QDB2/JVM");
    }
    
    private void buildMGRLVLLS(int agent, int sqlam, int rdb, int secmgr, int xamgr, int syncptmgr, int rsyncmgr,
            int unicodemgr) throws SQLException {
        markLengthBytes(CodePoint.MGRLVLLS);

        // place the managers and their levels in the buffer
        buffer.writeShort(CodePoint.AGENT);
        buffer.writeShort(agent);
        buffer.writeShort(CodePoint.SQLAM);
        buffer.writeShort(sqlam);
        buffer.writeShort(CodePoint.UNICODEMGR);
        buffer.writeShort(unicodemgr);
        buffer.writeShort(CodePoint.RDB);
        buffer.writeShort(rdb);
        buffer.writeShort(CodePoint.SECMGR);
        buffer.writeShort(secmgr);
        buffer.writeShort(CodePoint.CMNTCPIP);// @AGG added
        buffer.writeShort(0x08);

        // @AGG removed
        // if (netAgent_.netConnection_.isXAConnection()) {
        // if (xamgr != NetConfiguration.MGRLVL_NA) {
        // writeCodePoint4Bytes(CodePoint.XAMGR, xamgr);
        // }
        // if (syncptmgr != NetConfiguration.MGRLVL_NA) {
        // writeCodePoint4Bytes(CodePoint.SYNCPTMGR, syncptmgr);
        // }
        // if (rsyncmgr != NetConfiguration.MGRLVL_NA) {
        // writeCodePoint4Bytes(CodePoint.RSYNCMGR, rsyncmgr);
        // }
        // }
        updateLengthBytes();
    }
    
    // The External Name is the name of the job, task, or process on a
    // system for which a DDM server is active.
    private void buildEXTNAM(String extnam) throws SQLException {
        final int MAX_SIZE = 255;
        int extnamTruncateLength = Math.min(extnam.length(),MAX_SIZE);

        // Writing the truncated string as to preserve previous behavior
        writeScalarString(CodePoint.EXTNAM, extnam.substring(0, extnamTruncateLength), 141, // @AGG changed min byte len 0->141
                MAX_SIZE, "NET_EXTNAM_TOO_LONG");
    }
    
    // Server Name is the name of the DDM server.
    private void buildSRVNAM(String srvnam) throws SQLException {
        final int MAX_SIZE = 255;
        int srvnamTruncateLength = Math.min(srvnam.length(),MAX_SIZE);
        
        // Writing the truncated string as to preserve previous behavior
        writeScalarString(CodePoint.SRVNAM,srvnam.substring(0, srvnamTruncateLength),
                0, MAX_SIZE, "SQLState.NET_SRVNAM_TOO_LONG");
    }
    
    // Server Product Release Level String specifies the product
    // release level of a DDM server.
    private void buildSRVRLSLV() {
        writeScalarString(CodePoint.SRVRLSLV, DRDAConstants.PRDID);
    }
    
    private String getHostname() {
        try {
            return Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "localhost";
        }
    }
    
    // creates an request dss in the buffer to contain a ddm command
    // object.  calling this method means any previous dss objects in
    // the buffer are complete and their length and chaining bytes can
    // be updated appropriately.
    protected final void createCommand() {
        buildDss(false, false, false, DssConstants.GDSFMT_RQSDSS, ++correlationID_, false);
    }
    
    private final void buildDss(boolean dssHasSameCorrelator, boolean chainedToNextStructure,
            boolean nextHasSameCorrelator, int dssType, int corrId, boolean simpleFinalizeBuildingNextDss) {
        if (doesRequestContainData()) {
            if (simpleDssFinalize) {
                finalizeDssLength();
            } else {
                finalizePreviousChainedDss(dssHasSameCorrelator);
            }
        }

        // RQSDSS header is 6 bytes long: (ll)(Cf)(rc)
        buffer.ensureWritable(6);

        // Save the position of the length bytes, so they can be updated with a
        // different value at a later time.
        dssLengthLocation_ = buffer.writerIndex();
        // Dummy values for the DSS length (token ll above).
        // The correct length will be inserted when the DSS is finalized.
        buffer.writeShort((short) 0xFFFF);

        // Insert the mandatory 0xD0 (token C).
        buffer.writeByte((byte) 0xD0);

        // Insert the dssType (token f), which also tells if the DSS is chained
        // or not. See DSSFMT in the DRDA specification for details.
        if (chainedToNextStructure) {
            dssType |= DssConstants.GDSCHAIN;
            if (nextHasSameCorrelator) {
                dssType |= DssConstants.GDSCHAIN_SAME_ID;
            }
        }
        buffer.writeByte((byte) dssType);

        // Write the request correlation id (two bytes, token rc).
        // use method that writes a short
        buffer.writeShort((short) corrId);

        simpleDssFinalize = simpleFinalizeBuildingNextDss;
    }
    
    // used to finialize a dss which is already in the buffer
    // before another dss is built.  this includes updating length
    // bytes and chaining bits.
    private final void finalizePreviousChainedDss(boolean dssHasSameCorrelator) {

        finalizeDssLength();
        int pos = dssLengthLocation_ + 3;
        byte value = buffer.getByte(pos);
        value |= 0x40;
        if (dssHasSameCorrelator) // for blobs
        {
            value |= 0x10;
        }
        buffer.setByte(pos, value);
    }
    
    /**
     * Signal the completion of a DSS Layer A object.
     * <p>
     * The length of the DSS object will be calculated based on the difference
     * between the start of the DSS, saved in the variable
     * {@link #dssLengthLocation_}, and the current offset into the buffer which
     * marks the end of the data.
     * <p>
     * In the event the length requires the use of continuation DSS headers,
     * one for each 32k chunk of data, the data will be shifted and the
     * continuation headers will be inserted with the correct values as needed.
     * Note: In the future, we may try to optimize this approach
     * in an attempt to avoid these shifts.
     */
    private final void finalizeDssLength() {
        // calculate the total size of the dss and the number of bytes which would
        // require continuation dss headers.  The total length already includes the
        // the 6 byte dss header located at the beginning of the dss.  It does not
        // include the length of any continuation headers.
        int totalSize = buffer.writerIndex() - dssLengthLocation_;
        int bytesRequiringContDssHeader = totalSize - 32767;

        // determine if continuation headers are needed
        if (bytesRequiringContDssHeader > 0) {

            // the continuation headers are needed, so calculate how many.
            // after the first 32767 worth of data, a continuation header is
            // needed for every 32765 bytes (32765 bytes of data + 2 bytes of
            // continuation header = 32767 Dss Max Size).
            int contDssHeaderCount = bytesRequiringContDssHeader / 32765;
            if (bytesRequiringContDssHeader % 32765 != 0) {
                contDssHeaderCount++;
            }

            // right now the code will shift to the right.  In the future we may want
            // to try something fancier to help reduce the copying (maybe keep
            // space in the beginning of the buffer??).
            // the offset points to the next available offset in the buffer to place
            // a piece of data, so the last dataByte is at offset -1.
            // various bytes will need to be shifted by different amounts
            // depending on how many dss headers to insert so the amount to shift
            // will be calculated and adjusted as needed.  ensure there is enough room
            // for all the conutinuation headers and adjust the offset to point to the
            // new end of the data.
            int dataByte = buffer.writerIndex() - 1;
            int shiftOffset = contDssHeaderCount * 2;
            buffer.ensureWritable(shiftOffset);
            buffer.writerIndex(buffer.writerIndex() + shiftOffset);

            // mark passOne to help with calculating the length of the final (first or
            // rightmost) continuation header.
            boolean passOne = true;
            do {
                // calculate chunk of data to shift
                int dataToShift = bytesRequiringContDssHeader % 32765;
                if (dataToShift == 0) {
                    dataToShift = 32765;
                }

                // perform the shift
                dataByte -= dataToShift;
                byte[] array = buffer.array();
                System.arraycopy(array, dataByte + 1,
                        array, dataByte + shiftOffset + 1, dataToShift);

                // calculate the value the value of the 2 byte continuation dss header which
                // includes the length of itself.  On the first pass, if the length is 32767
                // we do not want to set the continuation dss header flag.
                int twoByteContDssHeader = dataToShift + 2;
                if (passOne) {
                    passOne = false;
                } else {
                    if (twoByteContDssHeader == 32767) {
                        twoByteContDssHeader = 0xFFFF;
                    }
                }

                // insert the header's length bytes
                buffer.setShort(dataByte + shiftOffset - 1,
                                (short) twoByteContDssHeader);

                // adjust the bytesRequiringContDssHeader and the amount to shift for
                // data in upstream headers.
                bytesRequiringContDssHeader -= dataToShift;
                shiftOffset -= 2;

                // shift and insert another header for more data.
            } while (bytesRequiringContDssHeader > 0);

            // set the continuation dss header flag on for the first header
            totalSize = 0xFFFF;

        }

        // insert the length bytes in the 6 byte dss header.
        buffer.setShort(dssLengthLocation_, (short) totalSize);
    }
    
    // Called to update the last ddm length bytes marked (lengths are updated
    // in the reverse order that they are marked).  It is up to the caller
    // to make sure length bytes were marked before calling this method.
    // If the length requires ddm extended length bytes, the data will be
    // shifted as needed and the extended length bytes will be automatically
    // inserted.
    protected final void updateLengthBytes() {
        // remove the top length location offset from the mark stack\
        // calculate the length based on the marked location and end of data.
        int lengthLocation = markStack.pop();
        int length = buffer.writerIndex() - lengthLocation;

        // determine if any extended length bytes are needed.  the value returned
        // from calculateExtendedLengthByteCount is the number of extended length
        // bytes required. 0 indicates no exteneded length.
        int extendedLengthByteCount = calculateExtendedLengthByteCount(length);
        if (extendedLengthByteCount != 0) {

            // ensure there is enough room in the buffer for the extended length bytes.
            ensureLength(extendedLengthByteCount);

            // calculate the length to be placed in the extended length bytes.
            // this length does not include the 4 byte llcp.
            int extendedLength = length - 4;

            // shift the data to the right by the number of extended length bytes needed.
            int extendedLengthLocation = lengthLocation + 4;
            byte[] array = buffer.array();
            System.arraycopy(array,
                    extendedLengthLocation,
                    array,
                    extendedLengthLocation + extendedLengthByteCount,
                    extendedLength);

            // write the extended length
            int shiftSize = (extendedLengthByteCount - 1) * 8;
            for (int i = 0; i < extendedLengthByteCount; i++) {
                buffer.setByte(extendedLengthLocation++,
                           (byte) (extendedLength >>> shiftSize));
                shiftSize -= 8;
            }
            // adjust the offset to account for the shift and insert
            buffer.writerIndex(buffer.writerIndex() + extendedLengthByteCount);

            // the two byte length field before the codepoint contains the length
            // of itself, the length of the codepoint, and the number of bytes used
            // to hold the extended length.  the 2 byte length field also has the first
            // bit on to indicate extended length bytes were used.
            length = extendedLengthByteCount + 4;
            length |= 0x8000;
        }

        // write the 2 byte length field (2 bytes before codepoint).
        buffer.setShort(lengthLocation, (short) length);
    }
    
    // this method writes a 4 byte length/codepoint pair plus the bytes contained
    // in array buff to the buffer.
    // the 2 length bytes in the llcp will contain the length of the data plus
    // the length of the llcp.  This method does not handle scenarios which
    // require extended length bytes.
    final void writeScalarBytes(int codePoint, byte[] buff) {
        writeScalarBytes(codePoint, buff, 0, buff.length);
    }

    // this method inserts a 4 byte length/codepoint pair plus length number of bytes
    // from array buff starting at offset start.
    // Note: no checking will be done on the values of start and length with respect
    // the actual length of the byte array.  The caller must provide the correct
    // values so an array index out of bounds exception does not occur.
    // the length will contain the length of the data plus the length of the llcp.
    // This method does not handle scenarios which require extended length bytes.
    final void writeScalarBytes(int codePoint, byte[] buff, int start, int length) {
        writeLengthCodePoint(length + 4, codePoint);
        ensureLength(length);
        buffer.writeBytes(buff, start, length);
    }
    
    final void writeScalarString(int codePoint, String string) {
        writeScalarString(codePoint, string, 0,Integer.MAX_VALUE,null);
    } 
    
    /**
     *  insert a 4 byte length/codepoint pair plus ddm character data into
     * the buffer.  This method assumes that the String argument can be
     * converted by the ccsid manager.  This should be fine because usually
     * there are restrictions on the characters which can be used for ddm
     * character data. 
     * The two byte length field will contain the length of the character data
     * and the length of the 4 byte llcp.  This method does not handle
     * scenarios which require extended length bytes.
     * 
     * @param codePoint  codepoint to write 
     * @param string     value
     * @param byteMinLength minimum length. String will be padded with spaces 
     * if value is too short. Assumes space character is one byte.
     * @param byteLengthLimit  Limit to string length. SQLException will be 
     * thrown if we exceed this limit.
     * @param sqlState  SQLState to throw with string as param if byteLengthLimit
     * is exceeded.
     * @throws SQLException if string exceeds byteLengthLimit
     */
    final void writeScalarString(int codePoint, String string, int byteMinLength,
            int byteLengthLimit, String sqlState) {
        // We don't know the length of the string yet, so set it to 0 for now.
        // Will be updated later.
        int lengthPos = buffer.writerIndex();
        writeLengthCodePoint(0, codePoint);

        int stringByteLength = encodeString(string);
        if (stringByteLength > byteLengthLimit) {
            throw new IllegalArgumentException("SQLState=" + sqlState + " " + string);
        }

        // pad if we don't reach the byteMinLength limit
        if (stringByteLength < byteMinLength) {
            padBytes(ccsidManager.getCCSID().encode(" ").get(), byteMinLength - stringByteLength);
            stringByteLength = byteMinLength;
        }

        // Update the length field. The length includes two bytes for the
        // length field itself and two bytes for the codepoint.
        buffer.setShort(lengthPos, (short) (stringByteLength + 4));
    }
    
    // helper method to calculate the minimum number of extended length bytes needed
    // for a ddm.  a return value of 0 indicates no extended length needed.
    private final int calculateExtendedLengthByteCount(long ddmSize) //throws SQLException
    {
        // according to Jim and some tests perfomred on Lob data,
        // the extended length bytes are signed.  Assume that
        // if this is the case for Lobs, it is the case for
        // all extended length scenarios.
        if (ddmSize <= 0x7FFF) {
            return 0;
        } else if (ddmSize <= 0x7FFFFFFFL) {
            return 4;
        } else if (ddmSize <= 0x7FFFFFFFFFFFL) {
            return 6;
        } else {
            return 8;
        }
    }
    
    /**
     * Encode a string and put it into the buffer. A larger buffer will be
     * allocated if the current buffer is too small to hold the entire string.
     *
     * @param string the string to encode
     * @return the number of bytes in the encoded representation of the string
     */
    private int encodeString(String string) {
        int startPos = buffer.writerIndex();
        buffer.writeCharSequence(string, ccsidManager.getCCSID());
        return buffer.writerIndex() - startPos;
    }
    
    // insert a 4 byte length/codepoint pair into the buffer.
    // total of 4 bytes inserted in buffer.
    // Note: the length value inserted in the buffer is the same as the value
    // passed in as an argument (this value is NOT incremented by 4 before being
    // inserted).
    final void writeLengthCodePoint(int length, int codePoint) {
        buffer.writeShort((short) length);
        buffer.writeShort((short) codePoint);
    }
    
    // insert a 4 byte length/codepoint pair and a 1 byte unsigned value into the buffer.
    // total of 5 bytes inserted in buffer.
    protected final void writeScalar1Byte(int codePoint, int value) {
        ensureLength(5);
        buffer.writeByte((byte) 0x00);
        buffer.writeByte((byte) 0x05);
        buffer.writeShort((short) codePoint);
        buffer.writeByte((byte) value);
    }
    
    final void writeScalar2Bytes(int codePoint, int value) {
        ensureLength(6);
        buffer.writeByte((byte) 0x00);
        buffer.writeByte((byte) 0x06);
        buffer.writeShort((short) codePoint);
        buffer.writeShort((short) value);
    }
    
    // mark the location of a two byte ddm length field in the buffer,
    // skip the length bytes for later update, and insert a ddm codepoint
    // into the buffer.  The value of the codepoint is not checked.
    // this length will be automatically updated when construction of
    // the ddm object is complete (see updateLengthBytes method).
    // Note: this mechanism handles extended length ddms.
    protected final void markLengthBytes(int codePoint) {
        ensureLength(4);

        // save the location of length bytes in the mark stack.
        markStack.push(buffer.writerIndex());

        // skip the length bytes and insert the codepoint
        buffer.writerIndex(buffer.writerIndex() + 2);
        buffer.writeShort((short) codePoint);
    }
    
    // insert the padByte into the buffer by length number of times.
    private final void padBytes(byte padByte, int length) {
        ensureLength(length);
        for (int i = 0; i < length; i++) {
            buffer.writeByte(padByte);
        }
    }
    
    private void ensureLength(int length) {
        buffer.ensureWritable(length);
    }
    
    // method to determine if any data is in the request.
    // this indicates there is a dss object already in the buffer.
    private final boolean doesRequestContainData() {
        return buffer.writerIndex() != 0;
    }

}

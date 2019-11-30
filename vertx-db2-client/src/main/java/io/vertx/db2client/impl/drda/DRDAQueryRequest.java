package io.vertx.db2client.impl.drda;

import java.sql.ResultSet;
import java.util.Arrays;

import io.netty.buffer.ByteBuf;

public class DRDAQueryRequest extends DRDARequest {
    
    private static final String COLLECTIONNAME = "NULLID";
    
    public static final int defaultFetchSize = 64;
    
    public DRDAQueryRequest(ByteBuf buffer, CCSIDManager ccsidManager) {
        super(buffer, ccsidManager);
    }
    
    // Write the message to preform a prepare into.
    // Once the SQL statement has been prepared, it is executed until the unit of work, in
    // which the PRPSQLSTT command was issued, ends.  An exception to this is if
    // Keep Dynamic is being used.
    //
    // preconditions:
    public void writePrepareDescribeOutput(String sql, String dbName, Section section) {
        // @AGG primary query method
        buildPRPSQLSTT(section,
                sql,
                dbName,
                true, //sendRtnsqlda
                true, //sendTypsqlda
                CodePoint.TYPSQLDA_X_OUTPUT);  //typsqlda

//        if (((NetStatement) materialStatement).statement_.cursorAttributesToSendOnPrepare_ != null) {
            //buildSQLATTRcommandData(((NetStatement) materialStatement).statement_.cursorAttributesToSendOnPrepare_);
        buildSQLATTRcommandData("FOR READ ONLY "); // @AGG assuming readonly (query)
//        }

        buildSQLSTTcommandData(sql);  // statement follows in sqlstt command data object
    }
    
    // Write the message to open a bound or prepared query without input parameters.
    // Check this-> For open query without input parameters
    public void writeOpenQuery(Section section,
            String dbName,
                               int fetchSize,
                               int resultSetType) {
        boolean sendQryrowset = checkSendQryrowset(fetchSize, resultSetType);
        fetchSize = checkFetchsize(fetchSize, resultSetType);

        // think about if there is a way we can call build ddm just passing ddm parameters and not passing the material ps object
        // maybe not, if sometimes we need to set the caches hanging off the ps object during the ddm build
        // maybe we can extricate conditionals in the build ddm logic outside
        buildOPNQRY(section, dbName, sendQryrowset, fetchSize);


        // may be able to merge this with firstContinueQuery_ and push above conditional to common
        // @AGG don't think we need this
//        ((NetStatement) materialStatement).qryrowsetSentOnOpnqry_ = sendQryrowset; // net-specific event
        // @AGG added this
        finalizeDssLength();
    }
    
    //----------------------helper methods----------------------------------------
    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    // Build the Open Query Command to open a query to a relational database.
    // At SQLAM >= 7 we can request the return of a DA, are there
    // scenarios where this should currently be done (it is not supported now)
    //
    // preconditions:
    //   the sqlam and/or prdid must support command and parameters passed to this method,
    //   method will not validate against the connection's level of support
    //
    private void buildOPNQRY(Section section,
            String dbName,
                     boolean sendQueryRowSet,
                     int fetchSize) {
        createCommand();
        markLengthBytes(CodePoint.OPNQRY);

        buildPKGNAMCSN(dbName, section);
        buildQRYBLKSZ();  // specify a hard coded query block size

        if (sendQueryRowSet) {
            buildMAXBLKEXT(-1);
            buildQRYROWSET(fetchSize);
        }

        // Tell the server to close forward-only result sets
        // implicitly when they are exhausted. The server will ignore
        // this parameter if the result set is scrollable.
        // @AGG assume server supports this
//        if (netAgent_.netConnection_.serverSupportsQryclsimp()) {
            buildQRYCLSIMP();
//        }

        updateLengthBytes();  // opnqry is complete
    }
    
    /**
     * Build QRYCLSIMP (Query Close Implicit). Query Close Implicit
     * controls whether the target server implicitly closes a
     * non-scrollable query upon end of data (SQLSTATE 02000).
     */
    private void buildQRYCLSIMP() {
        writeScalar1Byte(CodePoint.QRYCLSIMP, CodePoint.QRYCLSIMP_YES);
    }
    
    // Maximum Number of Extra Blocks specifies a limit on the number of extra
    // blocks of answer set data per result set that the requester is capable of
    // receiveing.
    // this value must be able to be contained in a two byte signed number.
    // there is a minimum value of 0.
    // a zero indicates that the requester is not capable of receiving extra
    // query blocks of answer set data.
    // there is a SPCVAL of -1.
    // a value of -1 indicates that the requester is capable of receiving
    // the entire result set.
    //
    // preconditions:
    //   sqlam must support this parameter on the command, method will not check.
    void buildMAXBLKEXT(int maxNumOfExtraBlocks) {
        if (maxNumOfExtraBlocks != 0) {
            writeScalar2Bytes(CodePoint.MAXBLKEXT, maxNumOfExtraBlocks);
        }
    }
    
    void buildQRYROWSET(int fetchSize) {
        writeScalar4Bytes(CodePoint.QRYROWSET, fetchSize);
    }
    
    // Query Block Size specifies the query block size for the reply
    // data objects and the reply messages being returned from this command.
    // this is a 4 byte unsigned binary number.
    // the sqlam 6 min value is 512 and max value is 32767.
    // this value was increased in later sqlam levels.
    // until the code is ready to support larger query block sizes,
    // it will always use DssConstants.MAX_DSS_LEN which is 32767.
    //
    // preconditions:
    //   sqlam must support this parameter for the command, method will not check.
    void buildQRYBLKSZ() {
        writeScalar4Bytes(CodePoint.QRYBLKSZ, DssConstants.MAX_DSS_LENGTH);
    }
    
    private int checkFetchsize(int fetchSize, int resultSetType) {
        // if fetchSize is not set for scrollable cursors, set it to the default fetchSize
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && fetchSize == 0) {
            fetchSize = defaultFetchSize;
        }
        return fetchSize;
    }
    
    private boolean checkSendQryrowset(int fetchSize, int resultSetType) {
        // if the cursor is forward_only, ignore the fetchSize and let the server return
        // as many rows as fit in the query block.
        // if the cursor is scrollable, send qryrowset if it is supported by the server
        boolean sendQryrowset = false;
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            sendQryrowset = true;
        }
        return sendQryrowset;
    }
    
    protected void buildSQLSTTcommandData(String sql) {
        createEncryptedCommandData();
//        int loc = buffer.position();
        markLengthBytes(CodePoint.SQLSTT);
        buildSQLSTT(sql);
        updateLengthBytes();
        // @AGG encryption
//        if (netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRIDDTA ||
//                netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRPWDDTA) {
//            encryptDataStream(loc);
//        }
    }
    
    protected void buildSQLATTRcommandData(String sql) {
        createEncryptedCommandData();
//        int loc = buffer.position();
        markLengthBytes(CodePoint.SQLATTR);
        buildSQLSTT(sql);
        updateLengthBytes();
        // TODO @AGG encryption
//        if (netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRIDDTA ||
//                netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRPWDDTA) {
//            encryptDataStream(loc);
//        }

    }
    
    private void buildNOCMorNOCS(String string) {
        if (string == null) {
            buffer.writeShort(0xffff);
//            write2Bytes(0xffff);
        } else {
            if (Typdef.typdef.isCcsidMbcSet()) {
            byte[] sqlBytes = string.getBytes(Typdef.typdef.getCcsidMbcEncoding());
//                byte[] sqlBytes = string.getBytes(netAgent_.typdef_.getCcsidMbcEncoding());
            buffer.writeByte(0x00);
//                write1Byte(0x00);
            buffer.writeInt(sqlBytes.length);
//                write4Bytes(sqlBytes.length);
            buffer.writeBytes(sqlBytes);
//                writeBytes(sqlBytes, sqlBytes.length);
            buffer.writeByte(0xff);
//                write1Byte(0xff);
            } else {
                byte[] sqlBytes = string.getBytes(Typdef.typdef.getCcsidSbcEncoding());
                buffer.writeByte(0xff);
                buffer.writeByte(0x00);
//                write1Byte(0xff);
//                write1Byte(0x00);
                buffer.writeInt(sqlBytes.length);
//                write4Bytes(sqlBytes.length);
                buffer.writeBytes(sqlBytes);
//                writeBytes(sqlBytes, sqlBytes.length);
//            }
            }
        }
    }

    // SQLSTTGRP : FDOCA EARLY GROUP
    // SQL Statement Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLSTATEMENT_m; PROTOCOL TYPE LVCM; ENVLID 0x40; Length Override 32767
    //   SQLSTATEMENT_s; PROTOCOL TYPE LVCS; ENVLID 0x34; Length Override 32767
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLSTATEMENT_m; PROTOCOL TYPE NOCM; ENVLID 0xCF; Length Override 4
    //   SQLSTATEMENT_s; PROTOCOL TYPE NOCS; ENVLID 0xCB; Length Override 4
    private void buildSQLSTTGRP(String string) {
        buildNOCMorNOCS(string);
    }

    // SQLSTT : FDOCA EARLY ROW
    // SQL Statement Row Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLSTTGRP; GROUP LID 0x5C; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void buildSQLSTT(String string) {
        buildSQLSTTGRP(string);
    }
    
    // Build the Prepare SQL Statement Command to dynamically binds an
    // SQL statement to a section in an existing relational database (RDB) package.
    //
    // preconditions:
    //   the sqlam and/or prdid must support command and parameters passed to this method,
    //   method will not validate against the connection's level of support
    private void buildPRPSQLSTT(Section section,
                        String sql,
                        String dbName,
                        boolean sendRtnsqlda,
                        boolean sendTypsqlda,
                        int typsqlda) {
        createCommand();
        markLengthBytes(CodePoint.PRPSQLSTT);

        buildPKGNAMCSN(dbName, section);
        if (sendRtnsqlda) {
            System.out.println("@AGG send rtn sql data");
            buildRTNSQLDA();
        }
        if (sendTypsqlda) {
            System.out.println("@AGG send typsqlda");
            buildTYPSQLDA(typsqlda);
        }

        updateLengthBytes();
    }
    
    // Type of SQL Descriptor Area.
    // This is a single byte signed number that specifies the type of sqlda to
    // return for the command.
    // below sqlam 7 there were two possible enumerated values for this parameter.
    // 0 (CodePoint.TYPSQLDA_STD_OUTPUT)- the default, indicates return the output sqlda.
    // 1 (CodePoint.TYPSQLDA_STD_INPUT) - indicates return the input sqlda.
    // the typsqlda was enhanced at sqlam 7 to support extened describe.
    // at sqlam 7 the following enumerated values are supported.
    // 0 (CodePoint.TYPSQLDA_STD_OUTPUT) - the default, standard output sqlda.
    // 1 (CodePoint.TYPSQLDA_STD_INPUT) - standard input sqlda.
    // 2 (CodePoint.TYPSQLDA_LIGHT_OUTPUT) - light output sqlda.
    // 3 (CodePoint.TYPSQLDA_LIGHT_INPUT) - light input sqlda.
    // 4 (CodePoint.TYPSQLDA_X_OUTPUT) - extended output sqlda.
    // 5 (CodePoint.TYPSQLDA_X_INPUT) - extended input sqlda.
    //
    // preconditions:
    //   sqlam or prdid must support this, method will not check.
    //   valid enumerated type must be passed to method, method will not check.
    private void buildTYPSQLDA(int typeSqlda) {
        // possibly inspect typeSqlda value and verify against sqlam level
        if (typeSqlda != CodePoint.TYPSQLDA_STD_OUTPUT) {
            writeScalar1Byte(CodePoint.TYPSQLDA, typeSqlda);
        }
    }
    
    // Return SQL Descriptor Area controls whether to return
    // an SQL descriptor area that applies to the SQL statement this command
    // identifies.  The target SQLAM obtains the SQL descriptor area by performing
    // an SQL DESCRIBE function on the statement after the statement has been
    // prepared.
    // The value TRUE, X'F1' (CodePoint.TRUE), indicates an SQLDA is returned
    // The value FALSE, X'F0' (CodePoint.FALSE), default, indicates an SQLDA is not returned.
    //
    // preconditions:
    //   sqlam must support this parameter for the command, method will not check.
    private void buildRTNSQLDA() {
        writeScalar1Byte(CodePoint.RTNSQLDA, CodePoint.TRUE);
    }
    
    // this specifies the fully qualified package name,
    // consistency token, and section number within the package being used
    // to execute the SQL.  If the connection supports reusing the previous
    // package information and this information is the same except for the section
    // number then only the section number needs to be sent to the server.
    void buildPKGNAMCSN(String dbName, Section section) {
//        if (!canCommandUseDefaultPKGNAMCSN()) { // Always true in derby code
            markLengthBytes(CodePoint.PKGNAMCSN);
            // If PKGNAMCBytes is already available, copy the bytes to the request buffer directly.
            if (section.getPKGNAMCBytes() != null) {
                writeStoredPKGNAMCBytes(section);
            } else {
                // Mark the beginning of PKGNAMCSN bytes.
                markForCachingPKGNAMCSN();
                buildCommonPKGNAMinfo(dbName, section);
                writeScalarPaddedBytes(
                        CCSIDManager.UTF8.encode("SYSLVL01").array(),
                        DRDAConstants.PKGCNSTKN_FIXED_LEN,
                        DRDAConstants.NON_CHAR_DDM_DATA_PAD_BYTE);
                // store the PKGNAMCbytes
                storePKGNAMCBytes(section);
            }
            buffer.writeShort(section.getSectionNumber());
            //write2Bytes(section.getSectionNumber());
            updateLengthBytes();
//        } else {
//            writeScalar2Bytes(CodePoint.PKGSN, section.getSectionNumber());
//        }
    }
    
    private void storePKGNAMCBytes(Section section) {
        // Get the locaton where we started writing PKGNAMCSN
        int startPos = popMarkForCachingPKGNAMCSN();
        byte[] b = new byte[buffer.writerIndex() - startPos];
        buffer.getBytes(startPos, b);
//        buffer.position(startPos);
//        buffer.get(b);
        section.setPKGNAMCBytes(b);
    }
    
    private void writeStoredPKGNAMCBytes(Section section) {
        buffer.writeBytes(section.getPKGNAMCBytes());
//        writeBytes(section.getPKGNAMCBytes());
    }
    
    // mark an offest into the buffer by placing the current offset value on
    // a stack.
    private final void mark() {
        markStack.push(buffer.writerIndex());
//        markStack_[top_++] = buffer.position();
    }

    // remove and return the top offset value from mark stack.
    private final int popMark() {
        return markStack.pop();
//        return markStack_[--top_];
    }

    protected final void markForCachingPKGNAMCSN() {
        mark();
    }

    protected final int popMarkForCachingPKGNAMCSN() {
        return popMark();
    }
    
    private void buildCommonPKGNAMinfo(String dbName, Section section) {
        String collectionToFlow = COLLECTIONNAME;
        // the scalar data length field may or may not be required.  it depends
        // on the level of support and length of the data.
        // check the lengths of the RDBNAM, RDBCOLID, and PKGID.
        // Determine if the lengths require an SCLDTALEN object.
        // Note: if an SQLDTALEN is required for ONE of them,
        // it is needed for ALL of them.  This is why this check is
        // up front.
        // the SQLAM level dictates the maximum size for
        // RDB Collection Identifier (RDBCOLID)
        // Relational Database Name (RDBNAM)
        // RDB Package Identifier (PKGID)
        int maxIdentifierLength = DRDAConstants.PKG_IDENTIFIER_MAX_LEN;
//        CcsidManager ccsidMgr = netAgent_.getCurrentCcsidManager();

        byte[] dbnameBytes = ccsidManager.getCCSID().encode(dbName).array(); 
//                ccsidMgr.convertFromJavaString(
//                netAgent_.netConnection_.databaseName_, netAgent_);

        byte[] collectionToFlowBytes = ccsidManager.getCCSID().encode(collectionToFlow).array(); 
//        ccsidMgr.convertFromJavaString(
//                collectionToFlow, netAgent_);

        byte[] pkgNameBytes = ccsidManager.getCCSID().encode(section.getPackageName()).array(); 
//                ccsidMgr.convertFromJavaString(
//                section.getPackageName(), netAgent_);
        
        //Maximum RDBNAM length would depend on the server level. 
        // Server under 10.11 support only 255 bytes but server
        // at 10.11 and higher support higher limit of 1024
        boolean scldtalenRequired =
                checkPKGNAMlengths(dbName, //netAgent_.netConnection_.databaseName_,
                dbnameBytes.length,
                DRDAConstants.RDBNAM_MAX_LEN,
                DRDAConstants.PKG_IDENTIFIER_FIXED_LEN);
//                (netAgent_.netConnection_.databaseMetaData_.serverSupportLongRDBNAM())? 
//                        DRDAConstants.RDBNAM_MAX_LEN 
//                        : DRDAConstants.PKG_IDENTIFIER_MAX_LEN,  
//                        DRDAConstants.PKG_IDENTIFIER_FIXED_LEN);

        if (!scldtalenRequired) {
            scldtalenRequired = checkPKGNAMlengths(collectionToFlow,
                    collectionToFlowBytes.length,
                    maxIdentifierLength,
                    DRDAConstants.PKG_IDENTIFIER_FIXED_LEN);
        }

        if (!scldtalenRequired) {
            scldtalenRequired = checkPKGNAMlengths(section.getPackageName(),
                    pkgNameBytes.length,
                    maxIdentifierLength,
                    DRDAConstants.PKG_IDENTIFIER_FIXED_LEN);
        }

        // the format is different depending on if an SCLDTALEN is required.
        if (!scldtalenRequired) {
            byte padByte = ccsidManager.getCCSID().encode(" ").get();
            writeScalarPaddedBytes(dbnameBytes,
                    DRDAConstants.PKG_IDENTIFIER_FIXED_LEN, padByte);
            writeScalarPaddedBytes(collectionToFlowBytes,
                    DRDAConstants.PKG_IDENTIFIER_FIXED_LEN, padByte);
            writeScalarPaddedBytes(pkgNameBytes,
                    DRDAConstants.PKG_IDENTIFIER_FIXED_LEN, padByte);
        } else {
            buildSCLDTA(dbnameBytes, DRDAConstants.PKG_IDENTIFIER_FIXED_LEN);
            buildSCLDTA(collectionToFlowBytes, DRDAConstants.PKG_IDENTIFIER_FIXED_LEN);
            buildSCLDTA(pkgNameBytes, DRDAConstants.PKG_IDENTIFIER_FIXED_LEN);
        }
    }
    
    private void buildSCLDTA(byte[] identifier, int minimumLength) {
        int length = Math.max(minimumLength, identifier.length);
        buffer.writeShort(length);
        //write2Bytes(length);
        //byte padByte = netAgent_.getCurrentCcsidManager().space_;
        byte padByte = ccsidManager.getCCSID().encode(" ").get();
        writeScalarPaddedBytes(identifier, length, padByte);
    }
    
    // throws an exception if lengths exceed the maximum.
    // returns a boolean indicating if SLCDTALEN is required.
    private boolean checkPKGNAMlengths(String identifier,
                                       int length,
                                       int maxIdentifierLength,
                                       int lengthRequiringScldta) {
        if (length > maxIdentifierLength) {
            throw new IllegalArgumentException("SQLState.LANG_IDENTIFIER_TOO_LONG " + length);
//            throw new SqlException(netAgent_.logWriter_,
//                new ClientMessageId(SQLState.LANG_IDENTIFIER_TOO_LONG),
//                identifier, maxIdentifierLength);
        }

        return (length > lengthRequiringScldta);
    }

}

package io.vertx.db2client.impl.drda;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.ArrayDeque;
import java.util.Deque;

import io.netty.buffer.ByteBuf;

public class DRDAResponse {
    
    private final ByteBuf buffer;
    private final CCSIDManager ccsidManager;

    private Deque<Integer> ddmCollectionLenStack = new ArrayDeque<>(4);
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
    
    public void readAccessSecurity(int securityMechanism) {
        startSameIdChainParse();
        parseACCSECreply(securityMechanism);
        endOfSameIdChainData();
//      agent_.checkForChainBreakingException_();
    }

    public void readExchangeServerAttributes() {
        startSameIdChainParse();
        parseEXCSATreply();
        endOfSameIdChainData();
//        agent_.checkForChainBreakingException_();
    }
    
    // NET only entry point
    public void readSecurityCheck() {
        startSameIdChainParse();
        parseSECCHKreply();
        endOfSameIdChainData();
    }
    
    public RDBAccessData readAccessDatabase() {
        startSameIdChainParse();
        RDBAccessData accessData = parseACCRDBreply();
        endOfSameIdChainData();
        return accessData;
//        agent_.checkForChainBreakingException_();
    }
    
    // Parse the reply for the Access RDB Command.
    // This method handles the parsing of all command replies and reply data
    // for the accrdb command.
    private RDBAccessData parseACCRDBreply() {
        int peekCP = peekCodePoint();
        if (peekCP != CodePoint.ACCRDBRM) {
            throw new IllegalStateException("Expected state ACCRDBRM but got " + Integer.toHexString(peekCP));
        }

        RDBAccessData accessData = parseACCRDBRM();
        parseInitialPBSD();
        peekCP = peekCodePoint();
        if (peekCP == END_OF_SAME_ID_CHAIN) {
            return accessData;
        }

        parseTypdefsOrMgrlvlovrs();
        NetSqlca netSqlca = parseSQLCARD(null);
        completeSqlca(netSqlca);
        return accessData;
    }
    
    // @AGG this was originally on the connection class
    private void completeSqlca(NetSqlca sqlca) {
        if (sqlca == null) {
        } else if (sqlca.getSqlCode() > 0) {
            System.out.println("[WARNING] Got SQLCode " + sqlca.getSqlCode());
//            accumulateWarning(new SqlWarning(agent_.logWriter_, sqlca));
        } else if (sqlca.getSqlCode() < 0) {
            throw new IllegalStateException("Got sqlcode " + sqlca.getSqlCode());
//            agent_.accumulateReadException(new SqlException(agent_.logWriter_, sqlca));
        }
    }
    
    private NetSqlca parseSQLCARD(NetSqlca[] rowsetSqlca) {
        parseLengthAndMatchCodePoint(CodePoint.SQLCARD);
        int ddmLength = getDdmLength();
        ensureBLayerDataInBuffer(ddmLength);
        NetSqlca netSqlca = parseSQLCARDrow(rowsetSqlca);
        adjustLengths(getDdmLength());
        return netSqlca;
    }
    
    // SQLCARD : FDOCA EARLY ROW
    // SQL Communications Area Row Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLCAGRP; GROUP LID 0x54; ELEMENT TAKEN 0(all); REP FACTOR 1

    NetSqlca parseSQLCARDrow(NetSqlca[] rowsetSqlca) {
        return parseSQLCAGRP(rowsetSqlca);
    }
    
    // SQLCAGRP : FDOCA EARLY GROUP
    // SQL Communcations Area Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLSTATE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    //   SQLERRPROC; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    //   SQLCAXGRP; PROTOCOL TYPE N-GDA; ENVLID 0x52; Length Override 0
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLSTATE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    //   SQLERRPROC; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    //   SQLCAXGRP; PROTOCOL TYPE N-GDA; ENVLID 0x52; Length Override 0
    //   SQLDIAGGRP; PROTOCOL TYPE N-GDA; ENVLID 0x56; Length Override 0
    private NetSqlca parseSQLCAGRP(NetSqlca[] rowsetSqlca) {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return null;
        }

        int sqlcode = readFastInt();
        byte[] sqlstate = readFastBytes(5);
        byte[] sqlerrproc = readFastBytes(8);
        NetSqlca netSqlca = null;

        netSqlca = new NetSqlca(sqlcode, sqlstate, sqlerrproc);
        parseSQLCAXGRP(netSqlca);

        if (DRDAConstants.TARGET_SQL_AM >= DRDAConstants.MGRLVL_7) {
            netSqlca.setRowsetRowCount(parseSQLDIAGGRP(rowsetSqlca));
        }

        return netSqlca;
    }
    
    // SQLDIAGGRP : FDOCA EARLY GROUP
    // SQL Diagnostics Group Description - Identity 0xD1
    // Nullable Group
    // SQLDIAGSTT; PROTOCOL TYPE N-GDA; ENVLID 0xD3; Length Override 0
    // SQLDIAGCN;  DRFA TYPE N-RLO; ENVLID 0xF6; Length Override 0
    // SQLDIAGCI;  PROTOCOL TYPE N-RLO; ENVLID 0xF5; Length Override 0
    private long parseSQLDIAGGRP(NetSqlca[] rowsetSqlca) {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return 0;
        }

        long row_count = parseSQLDIAGSTT(rowsetSqlca);
        parseSQLDIAGCI(rowsetSqlca);
        parseSQLDIAGCN();

        return row_count;
    }
    
    // SQL Diagnostics Condition Information Array - Identity 0xF5
    // SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    // SQLDCIROW; ROW LID 0xE5; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    private void parseSQLDIAGCI(NetSqlca[] rowsetSqlca) {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }
        int num = parseFastSQLNUMROW();
        if (num == 0) {
            resetRowsetSqlca(rowsetSqlca, 0);
        }

        // lastRow is the row number for the last row that had a non-null SQLCA.
        int lastRow = 1;
        for (int i = 0; i < num; i++) {
            lastRow = parseSQLDCROW(rowsetSqlca, lastRow);
        }
        resetRowsetSqlca(rowsetSqlca, lastRow + 1);
    }
    
    // SQL Diagnostics Condition Row - Identity 0xE5
    // SQLDCGRP; GROUP LID 0xD5; ELEMENT TAKEN 0(all); REP FACTOR 1
    private int parseSQLDCROW(NetSqlca[] rowsetSqlca, int lastRow) {
        return parseSQLDCGRP(rowsetSqlca, lastRow);
    }
    
    // SQL Diagnostics Condition Group Description
    //
    // SQLDCCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCSTATE; PROTOCOL TYPE FCS; ENVLID Ox30; Lengeh Override 5
    // SQLDCREASON; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCLINEN; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCROWN; PROTOCOL TYPE FD; ENVLID 0x0E; Lengeh Override 31
    // SQLDCER01; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER02; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER03; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER04; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCPART; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCPPOP; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCMSGID; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 10
    // SQLDCMDE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    // SQLDCPMOD; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    // SQLDCRDB; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    // SQLDCTOKS; PROTOCOL TYPE N-RLO; ENVLID 0xF7; Length Override 0
    // SQLDCMSG_m; PROTOCOL TYPE NVMC; ENVLID 0x3F; Length Override 32672
    // SQLDCMSG_S; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 32672
    // SQLDCCOLN_m; PROTOCOL TYPE NVCM ; ENVLID 0x3F; Length Override 255
    // SQLDCCOLN_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCCURN_m; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCCURN_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCPNAM_m; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCPNAM_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXGRP; PROTOCOL TYPE N-GDA; ENVLID 0xD3; Length Override 1
    private int parseSQLDCGRP(NetSqlca[] rowsetSqlca, int lastRow) {
        int sqldcCode = readFastInt(); // SQLCODE
        String sqldcState = readFastString(5, CCSIDManager.UTF8); // SQLSTATE
        int sqldcReason = readFastInt();  // REASON_CODE
        int sqldcLinen = readFastInt(); // LINE_NUMBER
        int sqldcRown = (int) readFastLong(); // ROW_NUMBER

        // save +20237 in the 0th entry of the rowsetSqlca's.
        // this info is going to be used when a subsequent fetch prior is issued, and if already
        // received a +20237 then we've gone beyond the first row and there is no need to
        // flow another fetch to the server.
        if (sqldcCode == 20237) {
            rowsetSqlca[0] = new NetSqlca(sqldcCode,
                    sqldcState,
                    null);
        } else {
            if (rowsetSqlca[sqldcRown] != null) {
                rowsetSqlca[sqldcRown].resetRowsetSqlca(sqldcCode,
                        sqldcState);
            } else {
                rowsetSqlca[sqldcRown] = new NetSqlca(sqldcCode,
                        sqldcState,
                        null);
            }
        }

        // reset all entries between lastRow and sqldcRown to null
        for (int i = lastRow + 1; i < sqldcRown; i++) {
            rowsetSqlca[i] = null;
        }

        skipFastBytes(47);
        String sqldcRdb = parseFastVCS(); // RDBNAM
        // skip the tokens for now, since we already have the complete message.
        parseSQLDCTOKS(); // MESSAGE_TOKENS
        String sqldcMsg = parseFastNVCMorNVCS(); // MESSAGE_TEXT

        // skip the following for now.
        skipFastNVCMorNVCS();  // COLUMN_NAME
        skipFastNVCMorNVCS();  // PARAMETER_NAME
        skipFastNVCMorNVCS();  // EXTENDED_NAMES

        parseSQLDCXGRP(); // SQLDCXGRP
        return sqldcRown;
    }
    
    // SQL Diagnostics Extended Names Group Description - Identity 0xD5
    // Nullable
    //
    // SQLDCXRDB_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 1024
    // SQLDCXSCH_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXNAM_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXTBLN_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXRDB_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 1024
    // SQLDCXSCH_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXNAM_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXTBLN_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    //
    // SQLDCXCRDB_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 1024
    // SQLDCXCSCH_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXCNAM_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXCRDB_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 1024
    // SQLDCXCSCH_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXCNAM_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    //
    // SQLDCXRRDB_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 1024
    // SQLDCXRSCH_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXRNAM_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXRRDB_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 1024
    // SQLDCXRSCH_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXRNAM_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    //
    // SQLDCXTRDB_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 1024
    // SQLDCXTSCH_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXTNAM_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXTRDB_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 1024
    // SQLDCXTSCH_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXTNAM_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    private void parseSQLDCXGRP() {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }
        skipFastNVCMorNVCS();  // OBJECT_RDBNAM
        skipFastNVCMorNVCS();  // OBJECT_SCHEMA
        skipFastNVCMorNVCS();  // SPECIFIC_NAME
        skipFastNVCMorNVCS();  // TABLE_NAME
        String sqldcxCrdb = parseFastVCS();        // CONSTRAINT_RDBNAM
        skipFastNVCMorNVCS();  // CONSTRAINT_SCHEMA
        skipFastNVCMorNVCS();  // CONSTRAINT_NAME
        parseFastVCS();        // ROUTINE_RDBNAM
        skipFastNVCMorNVCS();  // ROUTINE_SCHEMA
        skipFastNVCMorNVCS();  // ROUTINE_NAME
        parseFastVCS();        // TRIGGER_RDBNAM
        skipFastNVCMorNVCS();  // TRIGGER_SCHEMA
        skipFastNVCMorNVCS();  // TRIGGER_NAME
    }
    
    private String parseFastNVCMorNVCS() {
        String stringToBeSet = null;
        if (readFastUnsignedByte() != CodePoint.NULLDATA) {
            int vcm_length = readFastUnsignedShort();
            if (vcm_length > 0) {
                stringToBeSet = readFastString(vcm_length, ccsidManager.getCCSID());//netAgent_.targetTypdef_.getCcsidMbcEncoding());
            }
            if (readFastUnsignedByte() != CodePoint.NULLDATA) {
                throw new IllegalStateException("SQLState.NET_NVCM_NVCS_BOTH_NON_NULL");
//                agent_.accumulateChainBreakingReadExceptionAndThrow(
//                    new DisconnectException(agent_,
//                        new ClientMessageId(
//                            SQLState.NET_NVCM_NVCS_BOTH_NON_NULL)));
            }
        } else {
            if (readFastUnsignedByte() != CodePoint.NULLDATA) {
                int vcs_length = readFastUnsignedShort();
                if (vcs_length > 0) {
                    stringToBeSet = readFastString(vcs_length, ccsidManager.getCCSID());//netAgent_.targetTypdef_.getCcsidSbcEncoding());
                }
            }
        }
        return stringToBeSet;
    }
    
    // SQL Diagnostics Condition Token Array - Identity 0xF7
    // SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    // SQLTOKROW; ROW LID 0xE7; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    private void parseSQLDCTOKS() {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }
        int num = parseFastSQLNUMROW();
        for (int i = 0; i < num; i++) {
            parseSQLTOKROW();
        }
    }
    
    // SQL Diagnostics Token Row - Identity 0xE7
    // SQLTOKGRP; GROUP LID 0xD7; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void parseSQLTOKROW() {
        parseSQLTOKGRP();
    }

    // check on SQLTOKGRP format
    private void parseSQLTOKGRP() {
        skipFastNVCMorNVCS();
    }
    
    private void skipFastNVCMorNVCS() {
        if (readFastUnsignedByte() != CodePoint.NULLDATA) {
            int vcm_length = readFastUnsignedShort();
            if (vcm_length > 0)
            //stringToBeSet = readString (vcm_length, netAgent_.targetTypdef_.getCcsidMbcEncoding());
            {
                skipFastBytes(vcm_length);
            }
            if (readFastUnsignedByte() != CodePoint.NULLDATA) {
                throw new IllegalStateException("SQLState.NET_NVCM_NVCS_BOTH_NON_NULL");
//                agent_.accumulateChainBreakingReadExceptionAndThrow(
//                    new DisconnectException(agent_,
//                        new ClientMessageId(
//                            SQLState.NET_NVCM_NVCS_BOTH_NON_NULL)));
            }
        } else {
            if (readFastUnsignedByte() != CodePoint.NULLDATA) {
                int vcs_length = readFastUnsignedShort();
                if (vcs_length > 0)
                //stringToBeSet = readString (vcs_length, netAgent_.targetTypdef_.getCcsidSbcEncoding());
                {
                    skipFastBytes(vcs_length);
                }
            }
        }
    }
    
    private void resetRowsetSqlca(NetSqlca[] rowsetSqlca, int row) {
        // rowsetSqlca can be null.
        int count = ((rowsetSqlca == null) ? 0 : rowsetSqlca.length);
        for (int i = row; i < count; i++) {
            rowsetSqlca[i] = null;
        }
    }

    // SQL Diagnostics Connection Array - Identity 0xF6
    // SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    // SQLCNROW;  ROW LID 0xE6; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    private void parseSQLDIAGCN() {
        if (readUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }
        int num = parseFastSQLNUMROW();
        for (int i = 0; i < num; i++) {
            parseSQLCNROW();
        }
    }
    
    // SQL Diagnostics Connection Row - Identity 0xE6
    // SQLCNGRP; GROUP LID 0xD6; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void parseSQLCNROW() {
        parseSQLCNGRP();
    }
    
    // SQL Diagnostics Connection Group Description - Identity 0xD6
    // Nullable
    //
    // SQLCNSTATE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLCNSTATUS; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLCNATYPE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLCNETYPE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLCNPRDID; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    // SQLCNRDB; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 1024
    // SQLCNCLASS; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    // SQLCNAUTHID; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    private void parseSQLCNGRP() {
        skipBytes(18);
        String sqlcnRDB = parseFastVCS();    // RDBNAM
        String sqlcnClass = parseFastVCS();  // CLASS_NAME
        String sqlcnAuthid = parseFastVCS(); // AUTHID
    }
    
    // SQLNUMROW : FDOCA EARLY ROW
    // SQL Number of Elements Row Description
    //
    // FORMAT FOR SQLAM LEVELS
    //   SQLNUMGRP; GROUP LID 0x58; ELEMENT TAKEN 0(all); REP FACTOR 1
    int parseSQLNUMROW() {
        return parseSQLNUMGRP();
    }

    int parseFastSQLNUMROW() {
        return parseFastSQLNUMGRP();
    }

    // SQLNUMGRP : FDOCA EARLY GROUP
    // SQL Number of Elements Group Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLNUM; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    private int parseSQLNUMGRP() {
        return readShort();
    }

    private int parseFastSQLNUMGRP() {
        return readFastShort();
    }
    
    // SQL Diagnostics Statement Group Description - Identity 0xD3
    // Nullable Group
    // SQLDSFCOD; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSCOST; PROTOCOL TYPE I4; ENVLID 0X02; Length Override 4
    // SQLDSLROW; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSNPM; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSNRS; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSRNS; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSDCOD; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSROWC; PROTOCOL TYPE FD; ENVLID 0x0E; Length Override 31
    // SQLDSNROW; PROTOCOL TYPE FD; ENVLID 0x0E; Length Override 31
    // SQLDSROWCS; PROTOCOL TYPE FD; ENVLID 0x0E; Length Override 31
    // SQLDSACON; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACRH; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACRS; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACSL; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACSE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACTY; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSCERR; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSMORE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    private long parseSQLDIAGSTT(NetSqlca[] rowsetSqlca) {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return 0;
        }
        int sqldsFcod = readFastInt(); // FUNCTION_CODE
        int sqldsCost = readFastInt(); // COST_ESTIMATE
        int sqldsLrow = readFastInt(); // LAST_ROW

        skipFastBytes(16);

        long sqldsRowc = readFastLong(); // ROW_COUNT

        skipFastBytes(24);

        return sqldsRowc;
    }
    
    // SQLCAXGRP : EARLY FDOCA GROUP
    // SQL Communications Area Exceptions Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLRDBNME; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 18
    //   SQLERRD1; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD2; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD3; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD4; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD5; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLWARN0; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN1; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN2; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN3; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN4; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN5; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN6; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN7; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN8; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN9; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLERRMSG_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 70
    //   SQLERRMSG_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 70
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLERRD1; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD2; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD3; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD4; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD5; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLWARN0; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN1; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN2; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN3; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN4; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN5; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN6; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN7; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN8; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN9; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLRDBNAME; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 1024
    //   SQLERRMSG_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 70
    //   SQLERRMSG_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 70
    private void parseSQLCAXGRP(NetSqlca netSqlca) {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            netSqlca.setContainsSqlcax(false);
            return;
        }

//        if (DRDAConstants.TARGET_SQL_AM < DRDAConstants.MGRLVL_7) {
//            // skip over the rdbnam for now
//            //   SQLRDBNME; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 18
//            skipFastBytes(18);
//        }
        //   SQLERRD1 to SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
        int[] sqlerrd = new int[ NetSqlca.SQL_ERR_LENGTH ];
        readFastIntArray(sqlerrd);

        //   SQLWARN0 to SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
        byte[] sqlwarn = readFastBytes(11);

        if (DRDAConstants.TARGET_SQL_AM >= DRDAConstants.MGRLVL_7) {
            // skip over the rdbnam for now
            // SQLRDBNAME; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 1024
            parseFastVCS();
        }


        int sqlerrmcCcsid;
        byte[] sqlerrmc = readFastLDBytes();
        if (sqlerrmc != null) {
            sqlerrmcCcsid = ccsidManager.getCCSIDNumber();//  netAgent_.targetTypdef_.getCcsidMbc();
            skipFastBytes(2);
        } else {
            sqlerrmc = readFastLDBytes();
            sqlerrmcCcsid = ccsidManager.getCCSIDNumber(); //netAgent_.targetTypdef_.getCcsidSbc();
        }

        netSqlca.setSqlerrd(sqlerrd);
        netSqlca.setSqlwarnBytes(sqlwarn);
        netSqlca.setSqlerrmcBytes(sqlerrmc); // sqlerrmc may be null
    }
    
    private int parseTypdefsOrMgrlvlovrs() {
        boolean targetTypedefCloned = false;
        while (true) {
            int peekCP = peekCodePoint();
            if (peekCP == CodePoint.TYPDEFNAM) {
                if (!targetTypedefCloned) {
                    //netAgent_.targetTypdef_ = (Typdef) netAgent_.targetTypdef_.clone(); @AGG not sure if used?
                    targetTypedefCloned = true;
                }
                parseTYPDEFNAM();
            } else if (peekCP == CodePoint.TYPDEFOVR) {
                if (!targetTypedefCloned) {
                    //netAgent_.targetTypdef_ = (Typdef) netAgent_.targetTypdef_.clone(); @AGG not sure if used?
                    targetTypedefCloned = true;
                }
                parseTYPDEFOVR();
            } else {
                return peekCP;
            }
        }
    }
    
    /**
    * Parse the initial PBSD - PiggyBackedSessionData code point.
    * <p>
    * If sent by the server, it contains a PBSD_ISO code point followed by a
    * byte representing the JDBC isolation level, and a PBSD_SCHEMA code point
    * followed by the name of the current schema as an UTF-8 String.
    *
    * @throws org.apache.derby.client.am.DisconnectException
    */
   private void parseInitialPBSD() {
       if (peekCodePoint() != CodePoint.PBSD) {
           return;
       }
       parseLengthAndMatchCodePoint(CodePoint.PBSD);
       int peekCP = peekCodePoint();
       while (peekCP != END_OF_SAME_ID_CHAIN) {
           parseLengthAndMatchCodePoint(peekCP);
           switch (peekCP) {
               case CodePoint.PBSD_ISO:
                   int isolationLevel = readUnsignedByte();
                   if (isolationLevel != Connection.TRANSACTION_READ_COMMITTED)
                       throw new IllegalStateException("Database using unsupported transaction isolation level: " + isolationLevel);
//                   netAgent_.netConnection_.
//                       completeInitialPiggyBackIsolation(readUnsignedByte());
                   break;
               case CodePoint.PBSD_SCHEMA:
                   String pbSchema = readString(getDdmLength(), CCSIDManager.UTF8);
                   System.out.println("@AGG got piggyback schema: " + pbSchema);
//                   netAgent_.netConnection_.
//                       completeInitialPiggyBackSchema
//                           (readString(getDdmLength(), Typdef.UTF8ENCODING));
                   break;
               default:
                   throw new IllegalStateException("Found unknown codepoint: " + Integer.toHexString(peekCP));
           }
           peekCP = peekCodePoint();
       }
   }
    
    // Access to RDB Completed (ACRDBRM) Reply Message specifies that an
    // instance of the SQL application manager has been created and is bound
    // to the specified relation database (RDB).
    //
    // Returned from Server:
    // SVRCOD - required  (0 - INFO, 4 - WARNING)
    // PRDID - required
    // TYPDEFNAM - required (MINLVL 4) (QTDSQLJVM)
    // TYPDEFOVR - required
    // RDBINTTKN - optional
    // CRRTKN - optional
    // USRID - optional
    // SRVLST - optional (MINLVL 5)
    private RDBAccessData parseACCRDBRM() {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean prdidReceived = false;
        String prdid = null;
        boolean typdefnamReceived = false;
        boolean typdefovrReceived = false;
        boolean rdbinttknReceived = false;
        boolean crrtknReceived = false;
        byte[] crrtkn = null;
        boolean usridReceived = false;
        String usrid = null;

        parseLengthAndMatchCodePoint(CodePoint.ACCRDBRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                // severity code.  If the target SQLAM cannot support the typdefovr
                // parameter values specified for the double-byte and mixed-byte CCSIDs
                // on the corresponding ACCRDB command, then the severity code WARNING
                // is specified on the ACCRDBRM.
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_INFO, CodePoint.SVRCOD_WARNING);
                peekCP = peekCodePoint();
            }

            // this is the product release level of the target RDB server.
            if (peekCP == CodePoint.PRDID) {
                foundInPass = true;
                prdidReceived = checkAndGetReceivedFlag(prdidReceived);
                prdid = parsePRDID(false); // false means do not skip the bytes
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.TYPDEFNAM) {
                // this is the name of the data type to the data representation mapping
                // definitions tha the target SQLAM uses when sending reply data objects.
                foundInPass = true;
                typdefnamReceived = checkAndGetReceivedFlag(typdefnamReceived);
                parseTYPDEFNAM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.TYPDEFOVR) {
                // this is the single-byte, double-byte, and mixed-byte CCSIDs of the
                // scalar data arrays (SDA) in the identified data type to data representation
                // mapping definitions.
                foundInPass = true;
                typdefovrReceived = checkAndGetReceivedFlag(typdefovrReceived);
                parseTYPDEFOVR();
                peekCP = peekCodePoint();
            }
            
            if (peekCP == CodePoint.RDBINTTKN) {
                // @AGG added manually
                foundInPass = true;
                rdbinttknReceived = checkAndGetReceivedFlag(rdbinttknReceived);
                parseRDBINTTKN(false);
                peekCP = peekCodePoint();
            }


            if (peekCP == CodePoint.USRID) {
                // specifies the target defined user ID.  It is returned if the value of
                // TRGDFTRT is TRUE in ACCRDB.  Right now this driver always sets this
                // value to false so this should never get returned here.
                // if it is returned, it could be considered an error but for now
                // this driver will just skip the bytes.
                foundInPass = true;
                usridReceived = checkAndGetReceivedFlag(usridReceived);
                usrid = parseUSRID(true);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CRRTKN) {
                // carries information to correlate with the work being done on bahalf
                // of an application at the source and at the target server.
                // defualt value is ''.
                // this parameter is only retunred if an only if the CRRTKN parameter
                // is not received on ACCRDB.  We will rely on server to send us this
                // in ACCRDBRM
                foundInPass = true;
                crrtknReceived = checkAndGetReceivedFlag(crrtknReceived);
                crrtkn = parseCRRTKN(false);
                peekCP = peekCodePoint();
            }


            if (!foundInPass) {
                throw new IllegalStateException("Found unknown codepoint: " + Integer.toHexString(peekCP));
                //doPrmnsprmSemantics(peekCP);
            }
        }
        popCollectionStack();
        // check for the required instance variables.
        if (!svrcodReceived)
            throw new IllegalStateException("Did not find codepoint SVRCOD in reply data");
        if (!prdidReceived)
            throw new IllegalStateException("Did not find codepoint PRDID in reply data");
        if (!typdefnamReceived)
            throw new IllegalStateException("Did not find codepoint TYPDEFNAM in reply data");
        if (!typdefovrReceived)
            throw new IllegalStateException("Did not find codepoint TYPDEFOVR in reply data");
//        checkRequiredObjects(svrcodReceived,
//                prdidReceived,
//                typdefnamReceived,
//                typdefovrReceived);

//        rdbAccessed(svrcod,
//                prdid,
//                crrtknReceived,
//                crrtkn);
        return new RDBAccessData(svrcod, prdid, crrtknReceived, crrtkn);
    }
    
    public static class RDBAccessData {
        public final int svrcod;
        public final String prdid;
        public final byte[] correlationToken;
        public RDBAccessData(int svrcod, String prdid, boolean crrtknReceived, byte[] crrtkn) {
            this.svrcod = svrcod;
            this.prdid = prdid;
            correlationToken = crrtknReceived ? crrtkn : null;
        }
    }
    
    private byte[] parseRDBINTTKN(boolean skip) {
        parseLengthAndMatchCodePoint(CodePoint.RDBINTTKN);
        if (skip) {
            skipBytes();
            return null;
        }
        return readBytes();
    }
    
    // Correlation Token specifies a token that is conveyed between source
    // and target servers for correlating the processing between servers.
    private byte[] parseCRRTKN(boolean skip) {
        parseLengthAndMatchCodePoint(CodePoint.CRRTKN);
        if (skip) {
            skipBytes();
            return null;
        }
        return readBytes();
    }
    
    // The User Id specifies an end-user name.
    private String parseUSRID(boolean skip) {
        parseLengthAndMatchCodePoint(CodePoint.USRID);
        if (skip) {
            skipBytes();
            return null;
        }
        return readString();
    };
    
    private void parseTYPDEFOVR() {
        parseLengthAndMatchCodePoint(CodePoint.TYPDEFOVR);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.CCSIDSBC) {
                foundInPass = true;
                int ccsid = parseCCSIDSBC();
                //netAgent_.targetTypdef_.setCcsidSbc(parseCCSIDSBC());
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CCSIDDBC) {
                foundInPass = true;
                int ccsid = parseCCSIDDBC();
//                netAgent_.targetTypdef_.setCcsidDbc(parseCCSIDDBC());
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CCSIDMBC) {
                foundInPass = true;
                int ccsid = parseCCSIDMBC();
//                netAgent_.targetTypdef_.setCcsidMbc(parseCCSIDMBC());
                peekCP = peekCodePoint();
            }
            
            // @AGG added this block
            if (peekCP == CodePoint.CCSIDXML) {
                parseLengthAndMatchCodePoint(CodePoint.CCSIDXML);
                readUnsignedShort();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                throw new IllegalStateException("Found unknown codepoint: " + Integer.toHexString(peekCP));
                //doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
    }
    
    // CCSID for Single-Byte Characters specifies a coded character
    // set identifier for single-byte characters.
    private int parseCCSIDSBC() {
        parseLengthAndMatchCodePoint(CodePoint.CCSIDSBC);
        return readUnsignedShort();
    }

    // CCSID for Mixed-Byte Characters specifies a coded character
    // set identifier for mixed-byte characters.
    private int parseCCSIDMBC() {
        parseLengthAndMatchCodePoint(CodePoint.CCSIDMBC);
        return readUnsignedShort();
    }

    // CCSID for Double-Byte Characters specifies a coded character
    // set identifier for double-byte characters.
    private int parseCCSIDDBC() {
        parseLengthAndMatchCodePoint(CodePoint.CCSIDDBC);
        return readUnsignedShort();
    }
    
    private void parseTYPDEFNAM() {
        parseLengthAndMatchCodePoint(CodePoint.TYPDEFNAM);
        String typedef = readString();
//        netAgent_.targetTypdef_.setTypdefnam(readString());
    }
    
    // Product specific Identifier specifies the product release level
    // of a DDM server.
    private String parsePRDID(boolean skip) {
        parseLengthAndMatchCodePoint(CodePoint.PRDID);
        if (skip) {
            skipBytes();
            return null;
        } else {
            return readString();
        }
    }
    
    // Parse the reply for the Security Check Command.
    // This method handles the parsing of all command replies and reply data
    // for the secchk command.
    private void parseSECCHKreply() {
        if (peekCodePoint() != CodePoint.SECCHKRM) {
            throw new IllegalStateException("Expected CodePoint.SECCHKRM");
        }

        parseSECCHKRM();
        if (peekCodePoint() == CodePoint.SECTKN) {
            // rpydta used only if the security mechanism returns
            // a security token that must be sent back to the source system.
            // this is only used for DCSSEC.  In the case of DCESEC,
            // the sectkn must be returned as reply data if DCE is using
            // mutual authentication.
            // Need to double check what to map this to.  This is probably
            // incorrect but consider it a conversation protocol error
            // 0x03 - OBJDSS sent when not allowed.
            //parseSECTKN (true);
            parseSECTKN(false);
        }
    }
    
    // The Security Check (SECCHKRM) Reply Message indicates the acceptability
    // of the security information.
    // this method returns the security check code. it is up to the caller to check
    // the value of this return code and take the appropriate action.
    //
    // Returned from Server:
    // SVRCOD - required  (0 - INFO, 8 - ERROR, 16 -SEVERE)
    // SECCHKCD - required
    // SECTKN - optional, ignorable
    // SVCERRNO - optional
    private void parseSECCHKRM() {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean secchkcdReceived = false;
        int secchkcd = CodePoint.SECCHKCD_00;
        boolean sectknReceived = false;
        byte[] sectkn = null;

        parseLengthAndMatchCodePoint(CodePoint.SECCHKRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();
        
        while (peekCP != END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                // severity code.  it's value is dictated by the SECCHKCD.
                // right now it will not be checked that it is the correct value
                // for the SECCHKCD.  maybe this will be done in the future.
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_INFO, CodePoint.SVRCOD_SEVERE);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SECCHKCD) {
                // security check code. this specifies the state of the security information.
                // there is a relationship between this value and the SVRCOD value.
                // right now this driver will not check these values against each other.
                foundInPass = true;
                secchkcdReceived = checkAndGetReceivedFlag(secchkcdReceived);
                secchkcd = parseSECCHKCD();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SECTKN) {
                // security token.
                // used when mutual authentication of the source and target servers
                // is requested.  The architecture lists this as an instance variable
                // and also says that the SECTKN flows as reply data to the secchk cmd and
                // it must flow after the secchkrm message.  Right now this driver doesn't
                // support ay mutual authentication so it will be ignored (it is listed
                // as an ignorable instance variable in the ddm manual).
                foundInPass = true;
                sectknReceived = checkAndGetReceivedFlag(sectknReceived);
                sectkn = parseSECTKN(true);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                throw new IllegalStateException("Found unexpected codepoint: " + peekCP);
            }

        }
        popCollectionStack();
        // check for the required instance variables.
        if (!svrcodReceived)
            throw new IllegalStateException("Did not receive SVRCOD codepoint");
        if (!secchkcdReceived)
            throw new IllegalStateException("Did not receive SECCHKCD codepoint");
//        checkRequiredObjects(svrcodReceived, secchkcdReceived);

//        netConnection.securityCheckComplete(svrcod, secchkcd);
        if (secchkcd != CodePoint.SECCHKCD_00) {
            throw new IllegalArgumentException("Authentication failed");
        }
    }
    
    // Severity Code is an indicator of the severity of a condition
    // detected during the execution of a command.
    private int parseSVRCOD(int minSvrcod, int maxSvrcod) {
        parseLengthAndMatchCodePoint(CodePoint.SVRCOD);

        int svrcod = readUnsignedShort();
        if ((svrcod != CodePoint.SVRCOD_INFO) &&
                (svrcod != CodePoint.SVRCOD_WARNING) &&
                (svrcod != CodePoint.SVRCOD_ERROR) &&
                (svrcod != CodePoint.SVRCOD_SEVERE) &&
                (svrcod != CodePoint.SVRCOD_ACCDMG) &&
                (svrcod != CodePoint.SVRCOD_PRMDMG) &&
                (svrcod != CodePoint.SVRCOD_SESDMG)) {
            doValnsprmSemantics(CodePoint.SVRCOD, svrcod);
        }

        if (svrcod < minSvrcod || svrcod > maxSvrcod) {
            doValnsprmSemantics(CodePoint.SVRCOD, svrcod);
        }
        
        // @AGG remove this after fixing peekCP() ? 
        //adjustLengths(2); // @AGG had to add this after debugging

        return svrcod;
    }
    
    private void parseACCSECreply(int securityMechanism) {
        int peekCP = peekCodePoint();
        if (peekCP != CodePoint.ACCSECRD) {
            throw new IllegalStateException(String.format("Expecting ACCSECRD codepoint (0x14AC) but got %04x", peekCP));
            //parseAccessSecurityError(netConnection);
        }
        parseACCSECRD(securityMechanism);

//        peekCP = peekCodePoint();
//        if (SanityManager.DEBUG) {
//            if (peekCP != Reply.END_OF_SAME_ID_CHAIN) {
//                SanityManager.THROWASSERT("expected END_OF_SAME_ID_CHAIN");
//            }
//        }
    }
    
    // The Access Security Reply Data (ACSECRD) Collection Object contains
    // the security information from a target server's security manager.
    // this method returns the security check code received from the server
    // (if the server does not return a security check code, this method
    // will return 0).  it is up to the caller to check
    // the value of this return code and take the appropriate action.
    //
    // Returned from Server:
    // SECMEC - required
    // SECTKN - optional (MINLVL 6)
    // SECCHKCD - optional
    private void parseACCSECRD(int securityMechanism) {
        boolean secmecReceived = false;
        int[] secmecList = null;
        boolean sectknReceived = false;
        byte[] sectkn = null;
        boolean secchkcdReceived = false;
        int secchkcd = 0;

        parseLengthAndMatchCodePoint(CodePoint.ACCSECRD);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SECMEC) {
                // security mechanism.
                // this value must either reflect the value sent in the ACCSEC command
                // if the target server supports it; or the values the target server
                // does support when it does not support or accept the value
                // requested by the source server.
                // the secmecs returned are treated as a list and stored in
                // targetSecmec_List.
                // if the target server supports the source's secmec, it
                // will be saved in the variable targetSecmec_ (NOTE: so
                // after calling this method, if targetSecmec_'s value is zero,
                // then the target did NOT support the source secmec.  any alternate
                // secmecs would be contained in targetSecmec_List).
                foundInPass = true;
                secmecReceived = checkAndGetReceivedFlag(secmecReceived);
                secmecList = parseSECMEC();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SECTKN) {
                // security token
                foundInPass = true;
                sectknReceived = checkAndGetReceivedFlag(sectknReceived);
                sectkn = parseSECTKN(false);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SECCHKCD) {
                // security check code.
                // included if and only if an error is detected when processing
                // the ACCSEC command.  this has an implied severity code
                // of ERROR.
                foundInPass = true;
                secchkcdReceived = checkAndGetReceivedFlag(secchkcdReceived);
                secchkcd = parseSECCHKCD();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                throw new IllegalStateException("Found invalid codepoint: " + Integer.toHexString(peekCP));
                //doPrmnsprmSemantics(peekCP);
            }
        }
        popCollectionStack();
        if (!secmecReceived)
            throw new IllegalStateException("Did not receive SECMEC flag in response");
//        checkRequiredObjects(secmecReceived);

        // TODO: port this method to validate secMec received is same as requested one
        setAccessSecurityData(secchkcd,
                securityMechanism,
                secmecList,
                sectknReceived,
                sectkn);
        
        /* Switch to UTF-8 or EBCDIC managers depending on what's supported */
//        if (netConnection.serverSupportsUtf8Ccsid()) {
//            netConnection.netAgent_.switchToUtf8CcsidMgr();
//        } else {
//            netConnection.netAgent_.switchToEbcdicMgr();
//        }
        // ccsidManager.setCCSID(CCSIDManager.UTF8); // TODO @AGG should be switching to UTF8 here?
    }
    
    // secmecList is always required and will not be null.
    // secchkcd has an implied severity of error.
    // it will be returned if an error is detected.
    // if no errors and security mechanism requires a sectkn, then
    void setAccessSecurityData(int secchkcd,
                               int desiredSecmec,
                               int[] secmecList,
                               boolean sectknReceived,
                               byte[] sectkn) {
        // @AGG this method was originally on NetConnection
        
        // - if the secchkcd is not 0, then map to an exception.
        if (secchkcd != CodePoint.SECCHKCD_00) {
            // the implied severity code is error
            throw new IllegalStateException("Got nonzero SECCHKCD codepoint: " + secchkcd);
//            netAgent_.setSvrcod(CodePoint.SVRCOD_ERROR);
//            agent_.accumulateReadException(mapSecchkcd(secchkcd));
        } else {
            // - verify that the secmec parameter reflects the value sent
            // in the ACCSEC command.
            // should we check for null list
            if ((secmecList.length == 1) && (secmecList[0] == desiredSecmec)) {
                // the security mechanism returned from the server matches
                // the mechanism requested by the client.
//                targetSecmec_ = secmecList[0];

                if ((desiredSecmec == DRDAConstants.SECMEC_USRENCPWD) ||
                        (desiredSecmec == DRDAConstants.SECMEC_EUSRIDPWD) ||
                        (desiredSecmec == DRDAConstants.SECMEC_USRSSBPWD) ||
                        (desiredSecmec == DRDAConstants.SECMEC_EUSRIDDTA) ||
                        (desiredSecmec == DRDAConstants.SECMEC_EUSRPWDDTA)) {

                    // a security token is required for USRENCPWD, or EUSRIDPWD.
                    if (!sectknReceived) {
                        throw new IllegalStateException("SQLState.NET_SECTKN_NOT_RETURNED");
//                        agent_.accumulateChainBreakingReadExceptionAndThrow(
//                            new DisconnectException(agent_, 
//                                new ClientMessageId(SQLState.NET_SECTKN_NOT_RETURNED)));
                    } else {
                        throw new UnsupportedOperationException();
//                        if (desiredSecmec == NetConfiguration.SECMEC_USRSSBPWD)
//                            targetSeed_ = sectkn;
//                        else
//                            targetPublicKey_ = sectkn;
//                        if (encryptionManager_ != null) {
//                            encryptionManager_.resetSecurityKeys();
//                        }
                    }
                }
            } else {
                // accumulate an SqlException and don't disconnect yet
                // if a SECCHK was chained after this it would receive a secchk code
                // indicating the security mechanism wasn't supported and that would be a
                // chain breaking exception.  if no SECCHK is chained this exception
                // will be surfaced by endReadChain
                // agent_.accumulateChainBreakingReadExceptionAndThrow (
                //   new DisconnectException (agent_,"secmec not supported ","0000", -999));
                throw new IllegalStateException("SQLState.NET_SECKTKN_NOT_RETURNED");
//                agent_.accumulateReadException(new SqlException(agent_.logWriter_, 
//                    new ClientMessageId(SQLState.NET_SECKTKN_NOT_RETURNED)));
            }
        }
    }
    
    // The Security Check Code String codifies the security information
    // and condition for the SECCHKRM.
    private int parseSECCHKCD() {
        parseLengthAndMatchCodePoint(CodePoint.SECCHKCD);
        int secchkcd = readUnsignedByte();
        if ((secchkcd < CodePoint.SECCHKCD_00) || (secchkcd > CodePoint.SECCHKCD_15)) {
            doValnsprmSemantics(CodePoint.SECCHKCD, secchkcd);
        }
        // @AGG remove this after fixing PeekCP() ? 
        //adjustLengths(1); // @AGG added this after some debugging
        return secchkcd;
    }
    
    // The Security Token Byte String is information provided and used
    // by the various security mechanisms.
    private byte[] parseSECTKN(boolean skip) {
        parseLengthAndMatchCodePoint(CodePoint.SECTKN);
        if (skip) {
            skipBytes();
            return null;
        }
        return readBytes();
    }
    
    // Security Mechanims.
    private int[] parseSECMEC() {
        parseLengthAndMatchCodePoint(CodePoint.SECMEC);
        return readUnsignedShortList();
    }
    
    // Also called by NetStatementReply
    void doValnsprmSemantics(int codePoint, int value) {
        doValnsprmSemantics(codePoint, Integer.toString(value));
    }

    private void doValnsprmSemantics(int codePoint, String value) {

        // special case the FDODTA codepoint not to disconnect.
        if (codePoint == CodePoint.FDODTA) {
            throw new IllegalStateException("SQLState.DRDA_DDM_PARAMVAL_NOT_SUPPORTED codePoint=" + Integer.toHexString(codePoint));
//            agent_.accumulateReadException(new SqlException(agent_.logWriter_,
//                new ClientMessageId(SQLState.DRDA_DDM_PARAMVAL_NOT_SUPPORTED),
//                Integer.toHexString(codePoint)));
        }

        if (codePoint == CodePoint.CCSIDSBC ||
                codePoint == CodePoint.CCSIDDBC ||
                codePoint == CodePoint.CCSIDMBC) {
            // the server didn't like one of the ccsids.
            // the message should reflect the error in question.  right now these values
            // will be hard coded but this won't be correct if our driver starts sending
            // other values to the server.  In order to pick up the correct values,
            // a little reorganization may need to take place so that this code (or
            // whatever code sets the message) has access to the correct values.
            throw new IllegalStateException("SQLState.DRDA_NO_AVAIL_CODEPAGE_CONVERSION");
//            int cpValue = 0;
//            switch (codePoint) {
//            case CodePoint.CCSIDSBC:
//                cpValue = netAgent_.typdef_.getCcsidSbc();
//                break;
//            case CodePoint.CCSIDDBC:
//                cpValue = netAgent_.typdef_.getCcsidDbc();
//                break;
//            case CodePoint.CCSIDMBC:
//                cpValue = netAgent_.typdef_.getCcsidSbc();
//                break;
//            default:
//                // should never be in this default case...
//                break;
//            }
//            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
//                new ClientMessageId(SQLState.DRDA_NO_AVAIL_CODEPAGE_CONVERSION),
//                cpValue, value));
//            return;
        }
        // the problem isn't with one of the ccsid values so...

        throw new IllegalStateException("SQLState.DRDA_DDM_PARAMVAL_NOT_SUPPORTED");
        // Returning more information would
        // require rearranging this code a little.
//        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
//            new ClientMessageId(SQLState.DRDA_DDM_PARAMVAL_NOT_SUPPORTED),
//            Integer.toHexString(codePoint)));
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

    // Parse the reply for the Exchange Server Attributes Command.
    // This method handles the parsing of all command replies and reply data
    // for the excsat command.
    private void parseEXCSATreply() {
        if (peekCodePoint() != CodePoint.EXCSATRD) {
            parseExchangeServerAttributesError();
            return;
        }
        parseEXCSATRD();
    }
    
    // The Server Attributes Reply Data (EXCSATRD) returns the following
    // information in response to an EXCSAT command:
    // - the target server's class name
    // - the target server's support level for each class of manager
    //   the source requests
    // - the target server's product release level
    // - the target server's external name
    // - the target server's name
    //
    // Returned from Server:
    // EXTNAM - optional
    // MGRLVLLS - optional
    // SRVCLSNM - optional
    // SRVNAM - optional
    // SRVRLSLV - optional
    private void parseEXCSATRD() {
        boolean extnamReceived = false;
        boolean mgrlvllsReceived = false;
        boolean srvclsnmReceived = false;
        String srvclsnm = null;
        boolean srvnamReceived = false;
        boolean srvrlslvReceived = false;
        String srvrlslv = null;

        parseLengthAndMatchCodePoint(CodePoint.EXCSATRD);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();
        // EXTNAM = 71
        // MGRLVL = 28
        // SRVCLS = 19
        // SRVNAM = 9
        // SRVREL = 12
        // total = 139
        while (peekCP != END_OF_COLLECTION) {
            
            boolean foundInPass = false;

            if (peekCP == CodePoint.EXTNAM) {
                // External Name is the name of the job, task, or process
                // on a system for which a DDM server is active.  For a target
                // DDM server, the external name is the name of the job the system creates
                // or activates to run the DDM server.
                // No semantic meaning is assigned to external names in DDM.
                // External names are transmitted to aid in problem determination.
                foundInPass = true;
                extnamReceived = checkAndGetReceivedFlag(extnamReceived);
                parseLengthAndMatchCodePoint(CodePoint.EXTNAM);
                String extnam = readString();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.MGRLVLLS) {
                // Manager-Level List
                // specifies a list of code points and support levels for the
                // classes of managers a server supports
                foundInPass = true;
                mgrlvllsReceived = checkAndGetReceivedFlag(mgrlvllsReceived);
                parseMGRLVLLS();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVCLSNM) {
                // Server Class Name
                // specifies the name of a class of ddm servers.
                foundInPass = true;
                srvclsnmReceived = checkAndGetReceivedFlag(srvclsnmReceived);
                parseLengthAndMatchCodePoint(CodePoint.SRVCLSNM);
                srvclsnm = readString(); // parseSRVCLSNM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVNAM) {
                // Server Name
                // no semantic meaning is assigned to server names in DDM,
                // it is recommended (by the DDM manual) that the server's
                // physical or logical location identifier be used as a server name.
                // server names are transmitted for problem determination purposes.
                // this driver will save this name and in the future may use it
                // for logging errors.
                foundInPass = true;
                srvnamReceived = checkAndGetReceivedFlag(srvnamReceived);
                parseLengthAndMatchCodePoint(CodePoint.SRVNAM);
                readString();
                //parseSRVNAM(); // not used yet
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVRLSLV) {
                // Server Product Release Level
                // specifies the procuct release level of a ddm server.
                // the contents are unarchitected.
                // this driver will save this information and in the future may
                // use it for logging purposes.
                foundInPass = true;
                srvrlslvReceived = checkAndGetReceivedFlag(srvrlslvReceived);
                parseLengthAndMatchCodePoint(CodePoint.SRVRLSLV);
                srvrlslv = readString(); // parseSRVRLSLV();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                throw new IllegalStateException(String.format("Did not find a codepoint in this pass: %02x", peekCP));
                //doPrmnsprmSemantics(peekCP);
            }

        }
        
        ddmCollectionLenStack.pop();
        // according the the DDM book, all these instance variables are optional
        //netConnection.setServerAttributeData(srvclsnm, srvrlslv);
    }
    
    // Manager-Level List.
    // Specifies a list of code points and support levels for the
    // classes of managers a server supports.
    // The target server must not provide information for any target
    // managers unless the source explicitly requests it.
    // For each manager class, if the target server's support level
    // is greater than or equal to the source server's level, then the source
    // server's level is returned for that class if the target server can operate
    // at the source's level; otherwise a level 0 is returned.  If the target
    // server's support level is less than the source server's level, the
    // target server's level is returned for that class.  If the target server
    // does not recognize the code point of a manager class or does not support
    // that class, it returns a level of 0.  The target server then waits
    // for the next command or for the source server to terminate communications.
    // When the source server receives EXCSATRD, it must compare each of the entries
    // in the mgrlvlls parameter it received to the corresponding entries in the mgrlvlls
    // parameter it sent.  If any level mismatches, the source server must decide
    // whether it can use or adjust to the lower level of target support for that manager
    // class.  There are no architectural criteria for making this decision.
    // The source server can terminate communications or continue at the target
    // servers level of support.  It can also attempt to use whatever
    // commands its user requests while receiving eror reply messages for real
    // functional mismatches.
    // The manager levels the source server specifies or the target server
    // returns must be compatible with the manager-level dependencies of the specified
    // manangers.  Incompatible manager levels cannot be specified.
    // After this method successfully returns, the targetXXXX values (where XXXX
    // represents a manager name.  example targetAgent) contain the negotiated
    // manager levels for this particular connection.
    private void parseMGRLVLLS() {
        parseLengthAndMatchCodePoint(CodePoint.MGRLVLLS);

        // each manager class and level is 4 bytes long.
        // get the length of the mgrlvls bytes, make sure it contains
        // the correct number of bytes for a mgrlvlls object, and calculate
        // the number of manager's returned from the server.
        int managerListLength = getDdmLength();
        if ((managerListLength == 0) || ((managerListLength % 4) != 0)) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_OBJ_LEN_NOT_ALLOWED);
        }
        int managerCount = managerListLength / 4;
        
        // @AGG remove this after fixing peekCP() ? 
        //adjustLengths(managerListLength); // @AGG added this line in order to make parseEXCSATRD() parse lengths down to 0 properly

        // the managerCount should be equal to the same number of
        // managers sent on the excsat.

        // read each of the manager levels returned from the server.
        for (int i = 0; i < managerCount; i++) {

            // first two byte are the manager's codepoint, next two bytes are the level.
            int managerCodePoint = readUnsignedShort(); //buffer.readUnsignedShort(); //parseCODPNTDR();
            int managerLevel = readUnsignedShort(); //buffer.readUnsignedShort(); // parseMGRLVLN();

            // TODO: decide which manager levels we should support
            // check each manager to make sure levels are within proper limits
            // for this driver.  Also make sure unexpected managers are not returned.
//            switch (managerCodePoint) {
//
//            case CodePoint.AGENT:
//                if ((managerLevel < NetConfiguration.MIN_AGENT_MGRLVL) ||
//                        (managerLevel > netConnection.targetAgent_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetAgent_ = managerLevel;
//                break;
//
//            case CodePoint.CMNTCPIP:
//                if ((managerLevel < NetConfiguration.MIN_CMNTCPIP_MGRLVL) ||
//                        (managerLevel > netConnection.targetCmntcpip_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetCmntcpip_ = managerLevel;
//                break;
//
//            case CodePoint.RDB:
//                if ((managerLevel < NetConfiguration.MIN_RDB_MGRLVL) ||
//                        (managerLevel > netConnection.targetRdb_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetRdb_ = managerLevel;
//                break;
//
//            case CodePoint.SECMGR:
//                if ((managerLevel < NetConfiguration.MIN_SECMGR_MGRLVL) ||
//                        (managerLevel > netConnection.targetSecmgr_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetSecmgr_ = managerLevel;
//                break;
//
//            case CodePoint.SQLAM:
//                if ((managerLevel < NetConfiguration.MIN_SQLAM_MGRLVL) ||
//                        (managerLevel > netAgent_.targetSqlam_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netAgent_.orignalTargetSqlam_ = managerLevel;
//                break;
//
//            case CodePoint.CMNAPPC:
//                if ((managerLevel < NetConfiguration.MIN_CMNAPPC_MGRLVL) ||
//                        (managerLevel > netConnection.targetCmnappc_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetCmnappc_ = managerLevel;
//                break;
//
//            case CodePoint.XAMGR:
//                if ((managerLevel != 0) &&
//                        (managerLevel < NetConfiguration.MIN_XAMGR_MGRLVL) ||
//                        (managerLevel > netConnection.targetXamgr_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetXamgr_ = managerLevel;
//                break;
//
//            case CodePoint.SYNCPTMGR:
//                if ((managerLevel != 0) &&
//                        (managerLevel < NetConfiguration.MIN_SYNCPTMGR_MGRLVL) ||
//                        (managerLevel > netConnection.targetSyncptmgr_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetSyncptmgr_ = managerLevel;
//                break;
//            case CodePoint.UNICODEMGR:
//                if ((managerLevel < NetConfiguration.MIN_UNICODE_MGRLVL) ||
//                        (managerLevel > netConnection.targetUnicodemgr_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetUnicodemgr_ = managerLevel;
//                break;
//            case CodePoint.RSYNCMGR:
//                if ((managerLevel != 0) &&
//                        (managerLevel < NetConfiguration.MIN_RSYNCMGR_MGRLVL) ||
//                        (managerLevel > netConnection.targetRsyncmgr_)) {
//                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                }
//                netConnection.targetRsyncmgr_ = managerLevel;
//                break;
//                // The target server must not provide information for any target managers
//                // unless the source explicitly requests.  The following managers are never requested.
//            default:
//                doMgrlvlrmSemantics(managerCodePoint, managerLevel);
//                break;
//            }
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

    private void parseExchangeServerAttributesError() {
        int peekCP = peekCodePoint();
        throw new IllegalStateException(String.format("Invalid codepoint: %02x", peekCP));
        // switch (peekCP) {
        // case CodePoint.CMDCHKRM:
        // parseCMDCHKRM();
        // break;
        // case CodePoint.MGRLVLRM:
        // parseMGRLVLRM();
        // break;
        // default:
        // parseCommonError(peekCP);
        // }
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
    
    private void ensureBLayerDataInBuffer(int desiredDataSize) {
        // TODO: remove this after done porting
        ensureALayerDataInBuffer(desiredDataSize);
    }

    // Make sure a certain amount of Layer A data is in the buffer.
    // The data will be in the buffer after this method is called.
    private void ensureALayerDataInBuffer(int desiredDataSize) {
        if (buffer.readableBytes() < desiredDataSize) {
            throw new IllegalStateException(
                    "Needed to have " + desiredDataSize + " in buffer but only had " + buffer.readableBytes());
        }
    }

}

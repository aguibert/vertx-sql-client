package io.vertx.db2client.impl.drda;

import org.apache.derby.client.net.NetConfiguration;

public class DRDAConstants {
    // Value to use when padding non-character data in ddm objects.
    public static final byte NON_CHAR_DDM_DATA_PAD_BYTE = 0x00;
    
    // Maximum size of External Name.
    public static final int EXTNAM_MAXSIZE = 255;

    // Minimum agent level required by protocol.
    public static final int MIN_AGENT_MGRLVL = 3;

    // Minimum communications tcpip manager level required by protocol.
    public static final int MIN_CMNTCPIP_MGRLVL = 5;

    // Minimum LU6.2 Conversational Communications Manager
    public static final int MIN_CMNAPPC_MGRLVL = 3;

    // Minimum rdb manager level required by protocol.
    public static final int MIN_RDB_MGRLVL = 3;

    // Minimum secmgr manager level required by protocol.
    public static final int MIN_SECMGR_MGRLVL = 5;

    // Minimum sqlam manager level required by protocol.
    public static final int MIN_SQLAM_MGRLVL = 4;

    // Minimum xa manager level required by protocol.
    public static final int MIN_XAMGR_MGRLVL = 7;

    // Minimum secmgr manager level required by protocol.
    public static final int MIN_SYNCPTMGR_MGRLVL = 5;

    // Minimum sqlam manager level required by protocol.
    public static final int MIN_RSYNCMGR_MGRLVL = 5;

    // Minimum unicodemgr manager level required by protocol
    public static final int MIN_UNICODE_MGRLVL = 0;

    // Maximun Password size.
    public static final int PASSWORD_MAXSIZE = 255;

    // Fixed PRDDTA application id fixed length.
    public static final int PRDDTA_APPL_ID_FIXED_LEN = 20;

    // PRDDTA Accounting Suffix Length byte offset.
    public static final int PRDDTA_ACCT_SUFFIX_LEN_BYTE = 55;

    // PRDDTA Length byte offset.
    public static final int PRDDTA_LEN_BYTE = 0;

    // Maximum PRDDTA size.
    public static final int PRDDTA_MAXSIZE = 255;

    // PRDDTA platform id.
    public static final String PRDDTA_PLATFORM_ID = "JVM               ";

    // Fixed PRDDTA user id fixed length.
    public static final int PRDDTA_USER_ID_FIXED_LEN = 8;

    // Identifier Length for fixed length rdb name
    public static final int PKG_IDENTIFIER_FIXED_LEN = 18;

    // Maximum RDBNAM Identifier Length
    // this used to be 255 prior to DERBY-4805 fix
    public static final int RDBNAM_MAX_LEN = 1024;

    // Maximum RDB Identifier Length
    public static final int PKG_IDENTIFIER_MAX_LEN = 255;

    // Fixed pkgcnstkn length
    public static final int PKGCNSTKN_FIXED_LEN = 8;

    // Maximum length of a security token.
    // Anything greater than 32763 bytes of SECTKN would require extended length
    // DDMs.
    // This seems like an impossible upper bound limit right now so set
    // max to 32763 and cross bridge later.
    public static final int SECTKN_MAXSIZE = 32763; // this was 255

    // Server class name of the ClientDNC product.
    public static final String SRVCLSNM_JVM = "QDERBY/JVM";

    // Maximum size of SRVNAM Name.
    public static final int SRVNAM_MAXSIZE = 255;

    // Manager is NA or not usued.
    public static final int MGRLVL_NA = 0;

    // Manager Level 5 constant.
    public static final int MGRLVL_5 = 0x05;

    // Manager Level 7 constant.
    public static final int MGRLVL_7 = 0x07;
    
    // @AGG added for default targetSqlam
    public static final int TARGET_SQL_AM = 0x0B;

    // Indicates userid/encrypted password security mechanism.
    public static final int SECMEC_EUSRIDPWD = 0x09;

    // Indicates userid only security mechanism.
    public static final int SECMEC_USRIDONL = 0x04;

    // Indicates userid/encrypted password security mechanism.
    public static final int SECMEC_USRENCPWD = 0x07;

    // Indicates userid/password security mechanism.
    public static final int SECMEC_USRIDPWD = 0x03;

    // Indicates Encrypted userid and Encrypted Security-sensitive Data security
    // mechanism
    public static final int SECMEC_EUSRIDDTA = 0x0C;

    // Indicates Encrypted userid,Encrypted password and Encrypted
    // Security-sensitive Data security mechanism
    public static final int SECMEC_EUSRPWDDTA = 0x0D;

    // Indicates userid with strong password substitute security mechanism.
    public static final int SECMEC_USRSSBPWD = 0x08;

    // list of security mechanisms supported by this driver
    public static final int[] SECMGR_SECMECS = { NetConfiguration.SECMEC_EUSRIDPWD, NetConfiguration.SECMEC_USRENCPWD,
            NetConfiguration.SECMEC_USRIDPWD, NetConfiguration.SECMEC_USRIDONL, NetConfiguration.SECMEC_EUSRIDDTA,
            NetConfiguration.SECMEC_EUSRPWDDTA, NetConfiguration.SECMEC_USRSSBPWD };

    // IEEE ASCII constant.
    public static final String SYSTEM_ASC = "QTDSQLASC";

    // Maximum size of User Name.
    public static final int USRID_MAXSIZE = 255;

    // // Product id of the ClientDNC.
    public static final String PRDID = "JCC04250";
    
    public static final String EXTNAM = "db2jcc_application  " + PRDID + "300";
    //
    // // The server release level of this product.
    // // It will be prefixed with PRDID
    // static final String SRVRLSLV;
    
    private DRDAConstants() {
    }
}

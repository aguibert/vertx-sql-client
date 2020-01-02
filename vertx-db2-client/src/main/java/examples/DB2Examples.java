package examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import io.vertx.core.AsyncResult;
import io.vertx.db2client.DB2ConnectOptions;
import io.vertx.db2client.DB2Connection;
import io.vertx.db2client.DB2Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

/**
 * To Run DB2 in a docker container thaty is compatible with this sample, run
 * the script at scripts/db2.sh
 *
 * @author aguibert
 */
public class DB2Examples {

    public static void runJDBC() throws Exception {
        try (Connection con = DriverManager.getConnection("jdbc:db2://localhost:50000/vertx_db", "db2user", "db2pass")) {
             con.createStatement().execute("CREATE TABLE users ( id varchar(50) )");
             con.createStatement().execute("INSERT INTO users VALUES ('andy')");
             con.createStatement().execute("INSERT INTO users VALUES ('julien')");
             con.createStatement().execute("INSERT INTO users VALUES ('bob')");
             con.createStatement().execute("INSERT INTO users VALUES ('chuck')");
//            Statement stmt = con.createStatement();
            PreparedStatement ps = con.prepareStatement("SELECT * FROM users WHERE id=?");
            ps.setString(1, "andy");
            ResultSet rs = ps.executeQuery();
            // ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id='andy'");
//            ResultSet rs = stmt.executeQuery("SELECT * FROM users");
            while (rs.next())
                System.out.println("Got JDBC result: " + rs.getString(1));
        }
    }

    public static void main(String args[]) throws Exception {
//         runJDBC();
//         if (true)
//         return;

        // Connect options
        DB2ConnectOptions connectOptions = new DB2ConnectOptions()//
                .setPort(50000)//
                .setHost("localhost")//
                .setDatabase("vertx_db")//
                .setUser("db2user")//
                .setPassword("db2pass");

        // Pool options
        PoolOptions poolOptions = new PoolOptions().setMaxSize(5);

        // Create the client pool
        DB2Pool client = DB2Pool.pool(connectOptions, poolOptions);

        System.out.println("Created pool");
//        client.query("CREATE TABLE IF NOT EXISTS users ( id varchar(50) )", ar -> {
//            if (ar.succeeded()) {
//                System.out.println("Created table");
//            } else {
//                System.out.println("Create failed: " + ar.cause());
//            }
//        });
//
//        client.query("INSERT INTO users VALUES ('andy5')", ar -> {
//            System.out.println("INSERT results");
//            dumpResults(ar);
//        });
//        
//        client.query("SELECT * FROM users", ar2 -> {
//            System.out.println("SELECT results");
//            dumpResults(ar2);
//        });
//        client.query("SELECT * FROM users WHERE id='andy'", ar2 -> {
//            System.out.println("SELECT results");
//            dumpResults(ar2);
//        });
//        
//        client.query("DELETE FROM users WHERE id='andy5'", ar3 -> {
//            System.out.println("DELETE results");
//            dumpResults(ar3);
//        });
        
        client.preparedQuery("SELECT * FROM users WHERE id=?", Tuple.of("andy"), ar -> {
            System.out.println("@AGG inside PS lambda");
            dumpResults(ar);
          });
        
//        // TODO @AGG prepared statement with 2 query params not working yet
//        client.preparedQuery("SELECT * FROM users WHERE id=? OR id=?", Tuple.of("andy", "julien"), ar -> {
//            System.out.println("@AGG inside PS lambda");
//            dumpResults(ar);
//          });
        
        waitFor(2000);
        client.close();
        waitFor(500);
        System.out.println("Done");
    }
    
    private static void waitFor(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void dumpResults(AsyncResult<RowSet<Row>> ar) {
        try {
            if (ar.succeeded()) {
                RowSet<Row> result = ar.result();
                System.out.println("result=" + result);
                System.out.println("  rows=" + result.rowCount());
                System.out.println("  size=" + result.size());
                System.out.println(" names=" + result.columnsNames());
                for (Row row : result) {
                    System.out.println("  row=" + row);
                    System.out.println("    name=" + row.getColumnName(0));
                    System.out.println("    value=" + row.getString(0));
                }
            } else {
                System.out.println("Failure: " + ar.cause().getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void selectAll(DB2Pool client) {
        client.query("SELECT * FROM users", ar -> {
            try {
                if (ar.succeeded()) {
                    RowSet<Row> result = ar.result();
                    System.out.println("result=" + result);
                    System.out.println("  rows=" + result.rowCount());
                    System.out.println("  size=" + result.size());
                    System.out.println(" names=" + result.columnsNames());
                    for (Row row : result) {
                        System.out.println("  row=" + row);
                        System.out.println("    name=" + row.getColumnName(0));
                        System.out.println("    value=" + row.getString(0));
                    }
                } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });
    }

}
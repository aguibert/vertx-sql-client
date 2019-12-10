package examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.db2client.DB2ConnectOptions;
import io.vertx.db2client.DB2Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

/**
 * To Run DB2 in a docker container thaty is compatible with this sample, run
 * the command:
 * 
 * <pre>
 docker run --ulimit memlock=-1:-1 -it --rm=true --memory-swappiness=0 \
  --name db2-quarkus \
  -e DBNAME=quark_db \
  -e DB2INSTANCE=quark \
  -e DB2INST1_PASSWORD=quark \
  -e AUTOCONFIG=false \
  -e ARCHIVE_LOGS=false \
  -e LICENSE=accept \
  -p 50000:50000 \
  --privileged \
  ibmcom/db2:11.5.0.0a
 * </pre>
 *
 * @author aguibert
 *
 */
public class DB2Examples {

    public static void runJDBC() throws Exception {
        try (Connection con = DriverManager.getConnection("jdbc:db2://localhost:50000/quark_db", "quark", "quark")) {
            // con.createStatement().execute("CREATE TABLE users ( id varchar(50) )");
            // con.createStatement().execute("INSERT INTO users VALUES ('andy')");
            // con.createStatement().execute("INSERT INTO users VALUES ('julien')");
            // con.createStatement().execute("INSERT INTO users VALUES ('bob')");
            // con.createStatement().execute("INSERT INTO users VALUES ('chuck')");
            Statement stmt = con.createStatement();
            // ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id='andy'");
            ResultSet rs = stmt.executeQuery("SELECT * FROM users");
            while (rs.next())
                System.out.println("Got JDBC result: " + rs.getString(1));
        }
    }

    public static void main(String args[]) throws Exception {
        // runJDBC();
        // if (true)
        // return;

        // Connect options
        DB2ConnectOptions connectOptions = new DB2ConnectOptions().setPort(50000).setHost("localhost")
                .setDatabase("quark_db").setUser("quark").setPassword("quark");

        // Pool options
        PoolOptions poolOptions = new PoolOptions().setMaxSize(5);

        // Create the client pool
        DB2Pool client = DB2Pool.pool(connectOptions, poolOptions);

        System.out.println("Created pool");

        client.query("CREATE TABLE IF NOT EXISTS users ( id varchar(50) )", ar -> {
            if (ar.succeeded()) {
                System.out.println("Created table");
            } else {
                System.out.println("Create failed: " + ar.cause());
            }
        });

        client.query("INSERT INTO users VALUES ('andy5')", ar -> {
            System.out.println("INSERT results");
            dumpResults(ar);
        });
        
        client.query("SELECT * FROM users", ar2 -> {
            System.out.println("SELECT results");
            dumpResults(ar2);
        });
        
        client.query("DELETE FROM users WHERE id='andy5'", ar3 -> {
            System.out.println("DELETE results");
            dumpResults(ar3);
        });
        
        waitFor(750);
        client.close();
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

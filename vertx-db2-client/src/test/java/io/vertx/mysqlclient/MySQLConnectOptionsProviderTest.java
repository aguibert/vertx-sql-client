package io.vertx.mysqlclient;

import org.junit.Assert;
import org.junit.Test;

import io.vertx.db2client.DB2ConnectOptions;

public class MySQLConnectOptionsProviderTest {
  private String connectionUri;
  private DB2ConnectOptions expectedConfiguration;
  private DB2ConnectOptions actualConfiguration;

  @Test
  public void testValidUri1() {
    connectionUri = "mysql://localhost";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions();

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri2() {
    connectionUri = "mysql://myhost";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setHost("myhost");

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri3() {
    connectionUri = "mysql://myhost:3306";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setHost("myhost")
      .setPort(3306);

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri4() {
    connectionUri = "mysql://myhost/mydb";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setHost("myhost")
      .setDatabase("mydb");

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri5() {
    connectionUri = "mysql://user@myhost";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setUser("user")
      .setHost("myhost");

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri6() {
    connectionUri = "mysql://user:secret@myhost";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setUser("user")
      .setPassword("secret")
      .setHost("myhost");

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri7() {
    connectionUri = "mysql://other@localhost/otherdb?port=3306&password=secret";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setUser("other")
      .setPassword("secret")
      .setHost("localhost")
      .setPort(3306)
      .setDatabase("otherdb");

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri8() {
    connectionUri = "mariadb://other@localhost/otherdb?port=3306&password=secret";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setUser("other")
      .setPassword("secret")
      .setHost("localhost")
      .setPort(3306)
      .setDatabase("otherdb");

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri9() {
    connectionUri = "mysql://myhost?useAffectedRows=true";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setHost("myhost")
      .setUseAffectedRows(true);

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test
  public void testValidUri10() {
    connectionUri = "mysql://myhost?useAffectedRows=all_except_true_is_false";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);

    expectedConfiguration = new DB2ConnectOptions()
      .setHost("myhost")
      .setUseAffectedRows(false);

    assertEquals(expectedConfiguration, actualConfiguration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUri1() {
    connectionUri = "mysql://username:password@loc//dbname";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUri2() {
    connectionUri = "mysql://user@:passowrd@localhost/dbname/qwer";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUri3() {
    connectionUri = "mysql://user:password@localhost:655355/dbname";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUri4() {
    connectionUri = "mysql://user@localhost?port=1234&port";
    actualConfiguration = DB2ConnectOptions.fromUri(connectionUri);
  }

  private static void assertEquals(DB2ConnectOptions expectedConfiguration, DB2ConnectOptions actualConfiguration) {
    Assert.assertEquals(expectedConfiguration.toJson(), actualConfiguration.toJson());
  }
}

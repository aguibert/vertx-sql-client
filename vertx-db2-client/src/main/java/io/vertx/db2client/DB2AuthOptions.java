package io.vertx.db2client;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.db2client.impl.MySQLCollation;
import io.vertx.mysqlclient.MySQLAuthOptionsConverter;

import static io.vertx.db2client.DB2ConnectOptions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Authentication options for MySQL authentication which can be used for CHANGE_USER command.
 */
@DataObject(generateConverter = true)
public class DB2AuthOptions {
  private String user;
  private String password;
  private String database;
  private Map<String, String> properties;

  public DB2AuthOptions() {
    init();
  }

  public DB2AuthOptions(JsonObject json) {
    init();
    MySQLAuthOptionsConverter.fromJson(json, this);
  }

  public DB2AuthOptions(DB2AuthOptions other) {
    init();
    this.user = other.user;
    this.password = other.password;
    this.database = other.database;
    this.properties = new HashMap<>(other.properties);
  }

  /**
   * Get the user account to be used for the authentication.
   *
   * @return the user
   */
  public String getUser() {
    return user;
  }

  /**
   * Specify the user account to be used for the authentication.
   *
   * @param user the user to specify
   * @return a reference to this, so the API can be used fluently
   */
  public DB2AuthOptions setUser(String user) {
    Objects.requireNonNull(user, "User account can not be null");
    this.user = user;
    return this;
  }

  /**
   * Get the user password to be used for the authentication.
   *
   * @return the password
   */
  public String getPassword() {
    return password;
  }

  /**
   * Specify the user password to be used for the authentication.
   *
   * @param password the password to specify
   * @return a reference to this, so the API can be used fluently
   */
  public DB2AuthOptions setPassword(String password) {
    Objects.requireNonNull(password, "Password can not be null");
    this.password = password;
    return this;
  }

  /**
   * Get the database name for the re-authentication.
   *
   * @return the database name
   */
  public String getDatabase() {
    return database;
  }

  /**
   * Specify the default database for the re-authentication.
   *
   * @param database the database name to specify
   * @return a reference to this, so the API can be used fluently
   */
  public DB2AuthOptions setDatabase(String database) {
    Objects.requireNonNull(database, "Database name can not be null");
    this.database = database;
    return this;
  }

  /**
   * @return the value of current connection attributes
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Set connection attributes which will be sent to server at the re-authentication.
   *
   * @param properties the value of properties to specify
   * @return a reference to this, so the API can be used fluently
   */
  public DB2AuthOptions setProperties(Map<String, String> properties) {
    Objects.requireNonNull(properties, "Properties can not be null");
    this.properties = properties;
    return this;
  }

  /**
   * Add a property for this client, which will be sent to server at the re-authentication.
   *
   * @param key   the value of property key
   * @param value the value of property value
   * @return a reference to this, so the API can be used fluently
   */
  @GenIgnore
  public DB2AuthOptions addProperty(String key, String value) {
    Objects.requireNonNull(key, "Property key can not be null");
    Objects.requireNonNull(value, "Property value can not be null");
    this.properties.put(key, value);
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    MySQLAuthOptionsConverter.toJson(this, json);
    return json;
  }

  private void init() {
    this.user = DEFAULT_USER;
    this.password = DEFAULT_PASSWORD;
    this.database = DEFAULT_SCHEMA;
    this.properties = new HashMap<>(DEFAULT_CONNECTION_ATTRIBUTES);
  }
}

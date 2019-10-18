package io.vertx.db2client;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;
import io.vertx.db2client.impl.MySQLCollation;
import io.vertx.db2client.impl.DB2ConnectionUriParser;
import io.vertx.mysqlclient.MySQLConnectOptionsConverter;
import io.vertx.sqlclient.SqlConnectOptions;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Connect options for configuring {@link DB2Connection} or {@link DB2Pool}.
 */
@DataObject(generateConverter = true)
public class DB2ConnectOptions extends SqlConnectOptions {

  /**
   * Provide a {@link DB2ConnectOptions} configured from a connection URI.
   *
   * @param connectionUri the connection URI to configure from
   * @return a {@link DB2ConnectOptions} parsed from the connection URI
   * @throws IllegalArgumentException when the {@code connectionUri} is in an invalid format
   */
  public static DB2ConnectOptions fromUri(String connectionUri) throws IllegalArgumentException {
    JsonObject parsedConfiguration = DB2ConnectionUriParser.parse(connectionUri);
    return new DB2ConnectOptions(parsedConfiguration);
  }

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 50000;
  public static final String DEFAULT_USER = "root";
  public static final String DEFAULT_PASSWORD = "";
  public static final String DEFAULT_SCHEMA = "";
  public static final String DEFAULT_CHARSET = "utf8";
  public static final boolean DEFAULT_USE_AFFECTED_ROWS = false;
  public static final Map<String, String> DEFAULT_CONNECTION_ATTRIBUTES;

  static {
    Map<String, String> defaultAttributes = new HashMap<>();
    defaultAttributes.put("_client_name", "vertx-db2-client");
    DEFAULT_CONNECTION_ATTRIBUTES = Collections.unmodifiableMap(defaultAttributes);
  }

  public DB2ConnectOptions() {
    super();
  }

  public DB2ConnectOptions(JsonObject json) {
    super(json);
    MySQLConnectOptionsConverter.fromJson(json, this);
  }

  public DB2ConnectOptions(DB2ConnectOptions other) {
    super(other);
  }

  @Override
  public DB2ConnectOptions setHost(String host) {
    return (DB2ConnectOptions) super.setHost(host);
  }

  @Override
  public DB2ConnectOptions setPort(int port) {
    return (DB2ConnectOptions) super.setPort(port);
  }

  @Override
  public DB2ConnectOptions setUser(String user) {
    return (DB2ConnectOptions) super.setUser(user);
  }

  @Override
  public DB2ConnectOptions setPassword(String password) {
    return (DB2ConnectOptions) super.setPassword(password);
  }

  @Override
  public DB2ConnectOptions setDatabase(String database) {
    return (DB2ConnectOptions) super.setDatabase(database);
  }

  @Override
  public DB2ConnectOptions setCachePreparedStatements(boolean cachePreparedStatements) {
    return (DB2ConnectOptions) super.setCachePreparedStatements(cachePreparedStatements);
  }

  @Override
  public DB2ConnectOptions setPreparedStatementCacheMaxSize(int preparedStatementCacheMaxSize) {
    return (DB2ConnectOptions) super.setPreparedStatementCacheMaxSize(preparedStatementCacheMaxSize);
  }

  @Override
  public DB2ConnectOptions setPreparedStatementCacheSqlLimit(int preparedStatementCacheSqlLimit) {
    return (DB2ConnectOptions) super.setPreparedStatementCacheSqlLimit(preparedStatementCacheSqlLimit);
  }

  @Override
  public DB2ConnectOptions setProperties(Map<String, String> properties) {
    return (DB2ConnectOptions) super.setProperties(properties);
  }

  @GenIgnore
  @Override
  public DB2ConnectOptions addProperty(String key, String value) {
    return (DB2ConnectOptions) super.addProperty(key, value);
  }

  @Override
  public DB2ConnectOptions setSendBufferSize(int sendBufferSize) {
    return (DB2ConnectOptions) super.setSendBufferSize(sendBufferSize);
  }

  @Override
  public DB2ConnectOptions setReceiveBufferSize(int receiveBufferSize) {
    return (DB2ConnectOptions) super.setReceiveBufferSize(receiveBufferSize);
  }

  @Override
  public DB2ConnectOptions setReuseAddress(boolean reuseAddress) {
    return (DB2ConnectOptions) super.setReuseAddress(reuseAddress);
  }

  @Override
  public DB2ConnectOptions setReusePort(boolean reusePort) {
    return (DB2ConnectOptions) super.setReusePort(reusePort);
  }

  @Override
  public DB2ConnectOptions setTrafficClass(int trafficClass) {
    return (DB2ConnectOptions) super.setTrafficClass(trafficClass);
  }

  @Override
  public DB2ConnectOptions setTcpNoDelay(boolean tcpNoDelay) {
    return (DB2ConnectOptions) super.setTcpNoDelay(tcpNoDelay);
  }

  @Override
  public DB2ConnectOptions setTcpKeepAlive(boolean tcpKeepAlive) {
    return (DB2ConnectOptions) super.setTcpKeepAlive(tcpKeepAlive);
  }

  @Override
  public DB2ConnectOptions setSoLinger(int soLinger) {
    return (DB2ConnectOptions) super.setSoLinger(soLinger);
  }

  @Override
  public DB2ConnectOptions setIdleTimeout(int idleTimeout) {
    return (DB2ConnectOptions) super.setIdleTimeout(idleTimeout);
  }

  @Override
  public DB2ConnectOptions setIdleTimeoutUnit(TimeUnit idleTimeoutUnit) {
    return (DB2ConnectOptions) super.setIdleTimeoutUnit(idleTimeoutUnit);
  }

  @Override
  public DB2ConnectOptions setKeyCertOptions(KeyCertOptions options) {
    return (DB2ConnectOptions) super.setKeyCertOptions(options);
  }

  @Override
  public DB2ConnectOptions setKeyStoreOptions(JksOptions options) {
    return (DB2ConnectOptions) super.setKeyStoreOptions(options);
  }

  @Override
  public DB2ConnectOptions setPfxKeyCertOptions(PfxOptions options) {
    return (DB2ConnectOptions) super.setPfxKeyCertOptions(options);
  }

  @Override
  public DB2ConnectOptions setPemKeyCertOptions(PemKeyCertOptions options) {
    return (DB2ConnectOptions) super.setPemKeyCertOptions(options);
  }

  @Override
  public DB2ConnectOptions setTrustOptions(TrustOptions options) {
    return (DB2ConnectOptions) super.setTrustOptions(options);
  }

  @Override
  public DB2ConnectOptions setTrustStoreOptions(JksOptions options) {
    return (DB2ConnectOptions) super.setTrustStoreOptions(options);
  }

  @Override
  public DB2ConnectOptions setPemTrustOptions(PemTrustOptions options) {
    return (DB2ConnectOptions) super.setPemTrustOptions(options);
  }

  @Override
  public DB2ConnectOptions setPfxTrustOptions(PfxOptions options) {
    return (DB2ConnectOptions) super.setPfxTrustOptions(options);
  }

  @Override
  public DB2ConnectOptions addEnabledCipherSuite(String suite) {
    return (DB2ConnectOptions) super.addEnabledCipherSuite(suite);
  }

  @Override
  public DB2ConnectOptions addEnabledSecureTransportProtocol(String protocol) {
    return (DB2ConnectOptions) super.addEnabledSecureTransportProtocol(protocol);
  }

  @Override
  public DB2ConnectOptions removeEnabledSecureTransportProtocol(String protocol) {
    return (DB2ConnectOptions) super.removeEnabledSecureTransportProtocol(protocol);
  }

  @Override
  public DB2ConnectOptions setUseAlpn(boolean useAlpn) {
    return (DB2ConnectOptions) super.setUseAlpn(useAlpn);
  }

  @Override
  public DB2ConnectOptions setSslEngineOptions(SSLEngineOptions sslEngineOptions) {
    return (DB2ConnectOptions) super.setSslEngineOptions(sslEngineOptions);
  }

  @Override
  public DB2ConnectOptions setJdkSslEngineOptions(JdkSSLEngineOptions sslEngineOptions) {
    return (DB2ConnectOptions) super.setJdkSslEngineOptions(sslEngineOptions);
  }

  @Override
  public DB2ConnectOptions setTcpFastOpen(boolean tcpFastOpen) {
    return (DB2ConnectOptions) super.setTcpFastOpen(tcpFastOpen);
  }

  @Override
  public DB2ConnectOptions setTcpCork(boolean tcpCork) {
    return (DB2ConnectOptions) super.setTcpCork(tcpCork);
  }

  @Override
  public DB2ConnectOptions setTcpQuickAck(boolean tcpQuickAck) {
    return (DB2ConnectOptions) super.setTcpQuickAck(tcpQuickAck);
  }

  @Override
  public ClientOptionsBase setOpenSslEngineOptions(OpenSSLEngineOptions sslEngineOptions) {
    return super.setOpenSslEngineOptions(sslEngineOptions);
  }

  @Override
  public DB2ConnectOptions addCrlPath(String crlPath) throws NullPointerException {
    return (DB2ConnectOptions) super.addCrlPath(crlPath);
  }

  @Override
  public DB2ConnectOptions addCrlValue(Buffer crlValue) throws NullPointerException {
    return (DB2ConnectOptions) super.addCrlValue(crlValue);
  }

  @Override
  public DB2ConnectOptions setTrustAll(boolean trustAll) {
    return (DB2ConnectOptions) super.setTrustAll(trustAll);
  }

  @Override
  public DB2ConnectOptions setConnectTimeout(int connectTimeout) {
    return (DB2ConnectOptions) super.setConnectTimeout(connectTimeout);
  }

  @Override
  public DB2ConnectOptions setMetricsName(String metricsName) {
    return (DB2ConnectOptions) super.setMetricsName(metricsName);
  }

  @Override
  public DB2ConnectOptions setReconnectAttempts(int attempts) {
    return (DB2ConnectOptions) super.setReconnectAttempts(attempts);
  }

  @Override
  public DB2ConnectOptions setReconnectInterval(long interval) {
    return (DB2ConnectOptions) super.setReconnectInterval(interval);
  }

  @Override
  public DB2ConnectOptions setHostnameVerificationAlgorithm(String hostnameVerificationAlgorithm) {
    return (DB2ConnectOptions) super.setHostnameVerificationAlgorithm(hostnameVerificationAlgorithm);
  }

  @Override
  public DB2ConnectOptions setLogActivity(boolean logEnabled) {
    return (DB2ConnectOptions) super.setLogActivity(logEnabled);
  }

  @Override
  public DB2ConnectOptions setProxyOptions(ProxyOptions proxyOptions) {
    return (DB2ConnectOptions) super.setProxyOptions(proxyOptions);
  }

  @Override
  public DB2ConnectOptions setLocalAddress(String localAddress) {
    return (DB2ConnectOptions) super.setLocalAddress(localAddress);
  }

  @Override
  public DB2ConnectOptions setEnabledSecureTransportProtocols(Set<String> enabledSecureTransportProtocols) {
    return (DB2ConnectOptions) super.setEnabledSecureTransportProtocols(enabledSecureTransportProtocols);
  }

  @Override
  public DB2ConnectOptions setSslHandshakeTimeout(long sslHandshakeTimeout) {
    return (DB2ConnectOptions) super.setSslHandshakeTimeout(sslHandshakeTimeout);
  }

  @Override
  public DB2ConnectOptions setSslHandshakeTimeoutUnit(TimeUnit sslHandshakeTimeoutUnit) {
    return (DB2ConnectOptions) super.setSslHandshakeTimeoutUnit(sslHandshakeTimeoutUnit);
  }



  /**
   * Initialize with the default options.
   */
  protected void init() {
    this.setHost(DEFAULT_HOST);
    this.setPort(DEFAULT_PORT);
    this.setUser(DEFAULT_USER);
    this.setPassword(DEFAULT_PASSWORD);
    this.setDatabase(DEFAULT_SCHEMA);
    this.setProperties(new HashMap<>(DEFAULT_CONNECTION_ATTRIBUTES));
  }

  @Override
  public JsonObject toJson() {
    JsonObject json = super.toJson();
    MySQLConnectOptionsConverter.toJson(this, json);
    return json;
  }
}

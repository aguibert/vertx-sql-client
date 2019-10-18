package io.vertx.db2client;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.sqlclient.PropertyKind;

/**
 * An interface to define MySQL specific constants or behaviors.
 */
@VertxGen
public interface DB2Client {
  /**
   * SqlResult Property for last_insert_id
   */
  PropertyKind<Integer> LAST_INSERTED_ID = () -> Integer.class;
}

package io.vertx.db2client.impl.command;

import io.vertx.sqlclient.impl.command.CommandBase;

public class InitDbCommand extends CommandBase<Void> {
  private final String schemaName;

  public InitDbCommand(String schemaName) {
    this.schemaName = schemaName;
  }

  public String schemaName() {
    return schemaName;
  }
}

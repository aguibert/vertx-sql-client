package io.vertx.db2client.impl.command;

import io.vertx.db2client.MySQLSetOption;
import io.vertx.sqlclient.impl.command.CommandBase;

public class SetOptionCommand extends CommandBase<Void> {
  private final MySQLSetOption mySQLSetOption;

  public SetOptionCommand(MySQLSetOption mySQLSetOption) {
    this.mySQLSetOption = mySQLSetOption;
  }

  public MySQLSetOption option() {
    return mySQLSetOption;
  }
}

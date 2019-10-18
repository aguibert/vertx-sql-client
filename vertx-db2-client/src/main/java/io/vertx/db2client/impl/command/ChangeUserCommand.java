package io.vertx.db2client.impl.command;

import java.util.Map;

public class ChangeUserCommand extends AuthenticationCommandBase<Void> {
  public ChangeUserCommand(String username,
                           String password,
                           String database,
                           Map<String, String> connectionAttributes) {
    super(username, password, database, connectionAttributes);
  }
}

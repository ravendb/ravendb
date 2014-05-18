package net.ravendb.client.connection;

import net.ravendb.abstractions.json.linq.RavenJToken;

public class Operation {
  private long id;
  private RavenJToken state;
  private ServerClient client;

  public Operation(ServerClient client, long id) {
    super();
    this.id = id;
    this.client = client;
  }

  public Operation(long id, RavenJToken state) {
    super();
    this.id = id;
    this.state = state;
  }

  public RavenJToken waitForCompletion() {
    if (client == null)
      return state;

    while (true) {
      RavenJToken status = client.getOperationStatus(id);
      if (status == null) {
        return null;
      }
      if (status.value(Boolean.class, "Completed")) {
        return status.value(RavenJToken.class, "State");
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        /*ignore */
      }
    }
  }

}

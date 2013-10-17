package net.ravendb.abstractions.data;

import java.util.UUID;

public class BulkInsertChangeNotification extends DocumentChangeNotification {
  private UUID operationId;

  public UUID getOperationId() {
    return operationId;
  }


  public void setOperationId(UUID operationId) {
    this.operationId = operationId;
  }

}

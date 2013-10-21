package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.EventArgs;


public class ReplicationConflictNotification extends EventArgs {
  private ReplicationConflictTypes itemType;
  private String id;
  private Etag etag;
  private ReplicationOperationTypes operationType;
  private String[] conflicts;

  @Override
  public String toString() {
    return String.format("%s on %s because of %s operation", itemType, id, operationType);
  }

  public ReplicationConflictTypes getItemType() {
    return itemType;
  }

  public void setItemType(ReplicationConflictTypes itemType) {
    this.itemType = itemType;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Etag getEtag() {
    return etag;
  }

  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  public ReplicationOperationTypes getOperationType() {
    return operationType;
  }

  public void setOperationType(ReplicationOperationTypes operationType) {
    this.operationType = operationType;
  }

  public String[] getConflicts() {
    return conflicts;
  }

  public void setConflicts(String[] conflicts) {
    this.conflicts = conflicts;
  }

}

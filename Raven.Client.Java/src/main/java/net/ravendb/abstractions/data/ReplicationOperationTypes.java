package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.SerializeUsingValue;
import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
@SerializeUsingValue
public enum ReplicationOperationTypes {

  NONE(0),

  PUT(1),

  DELETE(2);

  private int value;

  public int getValue() {
    return value;
  }

  private ReplicationOperationTypes(int value) {
    this.value = value;
  }
}

package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.SerializeUsingValue;
import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
@SerializeUsingValue
public enum DocumentChangeTypes {
  NONE(0),

  PUT(1),
  DELETE(2),
  BULK_INSERT_STARTED(4),
  BULK_INSERT_ENDED(8),
  BULK_INSERT_ERROR(16),

  COMMON(3);

  private int value;

  private DocumentChangeTypes(int value) {
    this.value = value;
  }


  public int getValue() {
    return value;
  }


}

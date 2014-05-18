package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.SerializeUsingValue;
import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
@SerializeUsingValue
public enum TransformerChangeTypes {

  NONE(0),

  TRANSFORMER_ADDED(1),

  TRANSFORMER_REMOVED(2);

  private int value;

  private TransformerChangeTypes(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}

package net.ravendb.client;

import net.ravendb.abstractions.basic.SerializeUsingValue;
import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
@SerializeUsingValue
public enum SearchOptions  {

  OR(1),
  AND(2),
  NOT(4),
  GUESS(8);

  private SearchOptions(int value) {
    this.value = value;
  }

  private int value;

  public int getValue() {
    return value;
  }
}


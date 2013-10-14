package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.SerializeUsingValue;
import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
@SerializeUsingValue
public enum FacetAggregation{
  NONE(0),
  COUNT(1),
  MAX(2),
  MIN(4),
  AVERAGE(8),
  SUM(16);

  private FacetAggregation(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private int value;
}

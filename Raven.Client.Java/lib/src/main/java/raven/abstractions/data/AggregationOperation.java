package raven.abstractions.data;

import org.apache.commons.lang.StringUtils;

import raven.abstractions.basic.UseSharpEnum;
@UseSharpEnum
public enum AggregationOperation {
  NONE(0),

  COUNT(1),

  DISTINCT(1<<26),

  DYNAMIC(1<<27);


  private AggregationOperation(long value) {
    this.value = value;
  }

  private long value;

  public long getValue() {
    return value;
  }

  public String getStringValue() {
    return StringUtils.capitalize(toString().toLowerCase());
  }

}

package net.ravendb.abstractions.data;

import org.apache.commons.lang.StringUtils;

public class FacetValue {
  private String range;
  private int hits;
  private Double count;
  private Double sum;
  private Double max;
  private Double min;
  private Double average;

  public Double getAggregation(FacetAggregation aggregation) {
    switch(aggregation) {
    case NONE:
      return null;
    case COUNT:
      return count;
    case MAX:
      return max;
    case MIN:
      return min;
    case AVERAGE:
      return average;
    case SUM:
      return sum;
    default:
      return null;
    }
  }

  @Override
  public String toString() {
    String msg = range + " -  Hits: " + hits + ",";
    if (count != null) {
      msg += "Count: " + count + ",";
    }
    if(sum != null) {
      msg += "Sum: " + sum + ",";
    }
    if (max != null) {
      msg += "Max: " + max + ",";
    }
    if (min != null) {
      msg += "Min: " + min + ",";
    }
    if (average != null) {
      msg += "Average: " + average + ",";
    }

    msg = StringUtils.stripEnd(msg, " ,");
    return msg;
  }


  public Double getAverage() {
    return average;
  }
  public Double getCount() {
    return count;
  }
  public int getHits() {
    return hits;
  }
  public Double getMax() {
    return max;
  }
  public Double getMin() {
    return min;
  }
  public String getRange() {
    return range;
  }
  public Double getSum() {
    return sum;
  }
  public void setAverage(Double average) {
    this.average = average;
  }
  public void setCount(Double count) {
    this.count = count;
  }
  public void setHits(int hits) {
    this.hits = hits;
  }
  public void setMax(Double max) {
    this.max = max;
  }
  public void setMin(Double min) {
    this.min = min;
  }
  public void setRange(String range) {
    this.range = range;
  }
  public void setSum(Double sum) {
    this.sum = sum;
  }


}

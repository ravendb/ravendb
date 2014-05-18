package net.ravendb.abstractions.data;

public class BoostedValue {
  private float boost;
  private Object value;

  public float getBoost() {
    return boost;
  }
  public void setBoost(float boost) {
    this.boost = boost;
  }
  public Object getValue() {
    return value;
  }
  public void setValue(Object value) {
    this.value = value;
  }


}

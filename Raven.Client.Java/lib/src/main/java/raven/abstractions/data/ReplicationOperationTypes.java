package raven.abstractions.data;


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

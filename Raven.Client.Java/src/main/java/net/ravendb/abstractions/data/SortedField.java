package net.ravendb.abstractions.data;

/**
 *  Represent a field sort options
 */
public class SortedField {

  private String field;

  private boolean descending;
  public SortedField(String fieldWithPotentialPrefix) {
    if(fieldWithPotentialPrefix.startsWith("+")) {
      field = fieldWithPotentialPrefix.substring(1);
    } else if (fieldWithPotentialPrefix.startsWith("-")) {
      field = fieldWithPotentialPrefix.substring(1);
      descending = true;
    } else {
      field = fieldWithPotentialPrefix;
    }
  }

  @Override
  public SortedField clone() throws CloneNotSupportedException {
    return new SortedField((descending?"-":"")  + field);
  }
  public String getField() {
    return field;
  }
  public boolean isDescending() {
    return descending;
  }
  public void setDescending(boolean descending) {
    this.descending = descending;
  }
  public void setField(String field) {
    this.field = field;
  }

}

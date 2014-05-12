package net.ravendb.abstractions.data;


public class DocumentsChanges {

  private String fieldOldValue;
  private String fieldNewValue;
  private String fieldOldType;
  private String fieldNewType;
  private ChangeType change;
  private String fieldName;

  public String getFieldOldValue() {
    return fieldOldValue;
  }

  public void setFieldOldValue(String fieldOldValue) {
    this.fieldOldValue = fieldOldValue;
  }

  public String getFieldNewValue() {
    return fieldNewValue;
  }

  public void setFieldNewValue(String fieldNewValue) {
    this.fieldNewValue = fieldNewValue;
  }

  public String getFieldOldType() {
    return fieldOldType;
  }

  public void setFieldOldType(String fieldOldType) {
    this.fieldOldType = fieldOldType;
  }

  public String getFieldNewType() {
    return fieldNewType;
  }

  public void setFieldNewType(String fieldNewType) {
    this.fieldNewType = fieldNewType;
  }

  public ChangeType getChange() {
    return change;
  }

  public void setChange(ChangeType change) {
    this.change = change;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public static enum ChangeType {
    DOCUMENT_DELETED,
    DOCUMENT_ADDED,
    FIELD_CHANGED,
    NEW_FIELD,
    REMOVED_FIELD,
    ARRAY_VALUE_ADDED,
    ARRAY_VALUE_REMOVED;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((change == null) ? 0 : change.hashCode());
    result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
    result = prime * result + ((fieldNewType == null) ? 0 : fieldNewType.hashCode());
    result = prime * result + ((fieldNewValue == null) ? 0 : fieldNewValue.hashCode());
    result = prime * result + ((fieldOldType == null) ? 0 : fieldOldType.hashCode());
    result = prime * result + ((fieldOldValue == null) ? 0 : fieldOldValue.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    DocumentsChanges other = (DocumentsChanges) obj;
    if (change != other.change) return false;
    if (fieldName == null) {
      if (other.fieldName != null) return false;
    } else if (!fieldName.equals(other.fieldName)) return false;
    if (fieldNewType == null) {
      if (other.fieldNewType != null) return false;
    } else if (!fieldNewType.equals(other.fieldNewType)) return false;
    if (fieldNewValue == null) {
      if (other.fieldNewValue != null) return false;
    } else if (!fieldNewValue.equals(other.fieldNewValue)) return false;
    if (fieldOldType == null) {
      if (other.fieldOldType != null) return false;
    } else if (!fieldOldType.equals(other.fieldOldType)) return false;
    if (fieldOldValue == null) {
      if (other.fieldOldValue != null) return false;
    } else if (!fieldOldValue.equals(other.fieldOldValue)) return false;
    return true;
  }



}

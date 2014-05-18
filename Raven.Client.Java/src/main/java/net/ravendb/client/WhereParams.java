package net.ravendb.client;

/**
 * Parameters for the Where Equals call
 */
public class WhereParams {

  private String fieldName;
  private Object value;
  private boolean isAnalyzed;
  private Class<?> fieldTypeForIdentifier;
  private boolean allowWildcards;
  private boolean isNestedPath;

  /**
   * The field name
   * @return
   */
  public String getFieldName() {
    return fieldName;
  }
  /**
   * The field name
   * @param fieldName
   */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }
  /**
   * The field value
   * @return
   */
  public Object getValue() {
    return value;
  }

  /**
   * The field value
   * @param value
   */
  public void setValue(Object value) {
    this.value = value;
  }
  /**
   * Should the field be analyzed
   * @return
   */
  public boolean isAnalyzed() {
    return isAnalyzed;
  }

  /**
   * Should the field be analyzed
   * @param isAnalyzed
   */
  public void setAnalyzed(boolean isAnalyzed) {
    this.isAnalyzed = isAnalyzed;
  }

  public Class< ? > getFieldTypeForIdentifier() {
    return fieldTypeForIdentifier;
  }

  public void setFieldTypeForIdentifier(Class< ? > fieldTypeForIdentifier) {
    this.fieldTypeForIdentifier = fieldTypeForIdentifier;
  }

  /**
   * Should the field allow wildcards
   * @return
   */
  public boolean isAllowWildcards() {
    return allowWildcards;
  }

  /**
   * Should the field allow wildcards
   * @param allowWildcards
   */
  public void setAllowWildcards(boolean allowWildcards) {
    this.allowWildcards = allowWildcards;
  }

  /**
   * Is this a root property or not?
   * @return
   */
  public boolean isNestedPath() {
    return isNestedPath;
  }

  /**
   * Is this a root property or not?
   * @param isNestedPath
   */
  public void setNestedPath(boolean isNestedPath) {
    this.isNestedPath = isNestedPath;
  }

  /**
   *  Create a new instance
   */
  public WhereParams() {
    isNestedPath = false;
    allowWildcards = false;
    isAnalyzed = true;
  }
}

package raven.client.linq;

import java.beans.PropertyDescriptor;

/**
 * This class represents a node in an expression, usually a member - but in the case of dynamic queries the path to a member
 */
public class ExpressionInfo {
  private String path;
  private Class<?> clazz;
  private boolean nestedPath;
  private PropertyDescriptor maybeProperty;
  public String getPath() {
    return path;
  }
  public void setPath(String path) {
    this.path = path;
  }
  public Class<?> getClazz() {
    return clazz;
  }
  public void setClazz(Class<?> clazz) {
    this.clazz = clazz;
  }
  public boolean isNestedPath() {
    return nestedPath;
  }
  public void setNestedPath(boolean nestedPath) {
    this.nestedPath = nestedPath;
  }
  public PropertyDescriptor getMaybeProperty() {
    return maybeProperty;
  }
  public void setMaybeProperty(PropertyDescriptor maybeProperty) {
    this.maybeProperty = maybeProperty;
  }

  public ExpressionInfo(String path, Class<?> clazz, boolean nestedPath) {
    super();
    this.path = path;
    this.clazz = clazz;
    this.nestedPath = nestedPath;
  }


}

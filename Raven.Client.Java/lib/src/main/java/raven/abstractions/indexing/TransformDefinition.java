package raven.abstractions.indexing;

public class TransformDefinition implements Cloneable {
  public String transformResults;
  public String name;

  @Override
  public String toString() {
    if (name != null) {
      return name;
    }
    return transformResults;
  }


  @Override
  public Object clone() throws CloneNotSupportedException {
    TransformDefinition clone = new TransformDefinition();
    clone.setTransformResults(transformResults);
    clone.setName(name);
    return clone;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((transformResults == null) ? 0 : transformResults.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TransformDefinition other = (TransformDefinition) obj;
    if (transformResults == null) {
      if (other.transformResults != null)
        return false;
    } else if (!transformResults.equals(other.transformResults))
      return false;
    return true;
  }
  public String getTransformResults() {
    return transformResults;
  }
  public void setTransformResults(String transformResults) {
    this.transformResults = transformResults;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }


}

package net.ravendb.abstractions.exceptions;

public class IndexCompilationException extends RuntimeException {

  private String indexDefinitionProperty;
  private String problematicText;

  public IndexCompilationException() {
    super();
  }

  public IndexCompilationException(String message, Throwable cause) {
    super(message, cause);
  }

  public IndexCompilationException(String message) {
    super(message);
  }

  public IndexCompilationException(Throwable cause) {
    super(cause);
  }

  public String getIndexDefinitionProperty() {
    return indexDefinitionProperty;
  }

  public void setIndexDefinitionProperty(String indexDefinitionProperty) {
    this.indexDefinitionProperty = indexDefinitionProperty;
  }

  public String getProblematicText() {
    return problematicText;
  }

  public void setProblematicText(String problematicText) {
    this.problematicText = problematicText;
  }



}

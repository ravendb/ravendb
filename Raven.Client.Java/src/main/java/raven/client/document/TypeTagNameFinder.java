package raven.client.document;

/**
 * Finds type tag name based on class.
 */
public interface TypeTagNameFinder {

  public String find(Class<?> clazz);

}

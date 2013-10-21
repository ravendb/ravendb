package net.ravendb.abstractions.basic;

/**
 * Models out paramaters
 *
 * @param <T>
 */
public class Reference<T> {
  /**
   * The value contained in the holder.
   */
  public T value;

  /**
   * Creates a new holder with a <code>null</code> value.
   */
  public Reference() {
  }

  /**
   * Create a new holder with the specified value.
   *
   * @param value The value to be stored in the holder.
   */
  public Reference(T value) {
      this.value = value;
  }
}

package raven.abstractions.basic;

public class Holder<T> {
  /**
   * The value contained in the holder.
   */
  public T value;

  /**
   * Creates a new holder with a <code>null</code> value.
   */
  public Holder() {
  }

  /**
   * Create a new holder with the specified value.
   *
   * @param value The value to be stored in the holder.
   */
  public Holder(T value) {
      this.value = value;
  }
}

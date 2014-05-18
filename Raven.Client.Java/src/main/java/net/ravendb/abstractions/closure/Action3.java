package net.ravendb.abstractions.closure;

/**
 * Represents typed action with 3 arguments
 * @param <X>
 * @param <Y>
 * @param <Z>
 */
public interface Action3<X, Y, Z> {
  public void apply(X first, Y second, Z third);
}

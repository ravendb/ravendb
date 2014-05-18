package net.ravendb.abstractions.closure;

/**
 * Represents typed action with 2 arguments
 * @param <X>
 * @param <Y>
 */
public interface Action2<X, Y> {
  public void apply(X first, Y second);
}

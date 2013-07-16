package raven.abstractions.closure;

/**
 * Represents typed action with 3 arguments
 * @param <X>
 * @param <Y>
 * @param <Z>
 */
public interface Action2<X, Y> {
  public void apply(X first, Y second);
}

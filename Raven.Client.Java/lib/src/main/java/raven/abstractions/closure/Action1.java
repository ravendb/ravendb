package raven.abstractions.closure;

/**
 * Represents typed action with 3 arguments
 * @param <X>
 * @param <Y>
 * @param <Z>
 */
public interface Action1<X> {
  public void apply(X first);
}

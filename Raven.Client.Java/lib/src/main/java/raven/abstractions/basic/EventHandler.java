package raven.abstractions.basic;

public interface EventHandler<T> {
  public void handle(Object sender, T event);
}

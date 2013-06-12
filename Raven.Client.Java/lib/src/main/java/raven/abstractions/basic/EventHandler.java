package raven.abstractions.basic;

public interface EventHandler<T extends EventArgs> {
  public void handle(Object sender, T event);
}

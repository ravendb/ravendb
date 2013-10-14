package net.ravendb.abstractions.basic;

public interface EventHandler<T extends EventArgs> {
  /**
   * Handle event
   * @param sender
   * @param event
   */
  public void handle(Object sender, T event);
}

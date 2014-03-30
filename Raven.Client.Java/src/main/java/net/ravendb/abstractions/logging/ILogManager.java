package net.ravendb.abstractions.logging;

public interface ILogManager {
  public ILog getLogger(String name);
}

package net.ravendb.abstractions.closure;

public interface Function2<F, G, T> {
  /**
   * Applies function
   * @param input
   * @return
   */
  T apply(F first, G second);
}

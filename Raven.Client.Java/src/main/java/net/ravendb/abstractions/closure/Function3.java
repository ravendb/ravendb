package net.ravendb.abstractions.closure;

public interface Function3<F, G, H, T> {
  /**
   * Applies function
   * @param input
   * @return
   */
  T apply(F first, G second, H third);
}

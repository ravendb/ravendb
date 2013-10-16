package net.ravendb.abstractions.closure;

public interface Function4<F, G, H, I, T> {
  /**
   * Applies function
   * @param input
   * @return
   */
  T apply(F first, G second, H third, I fourth);
}

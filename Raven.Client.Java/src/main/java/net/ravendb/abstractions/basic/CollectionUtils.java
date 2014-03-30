package net.ravendb.abstractions.basic;

import java.util.Collection;

import net.ravendb.abstractions.closure.Function1;


public class CollectionUtils {

  public static <T> T firstOrDefault(Collection<? extends T> collection, Function1<T, Boolean> predicate) {
    for (T element: collection) {
      if (predicate.apply(element)) {
        return element;
      }
    }
    return null;
  }
}

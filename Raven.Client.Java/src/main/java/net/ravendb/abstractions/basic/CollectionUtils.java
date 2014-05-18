package net.ravendb.abstractions.basic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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

  public static <T> List<T> except(Iterable<T> source, Iterable<T> second) {

    List<T> secondAsList = new ArrayList<>();
    Iterator<T> secondIterator = second.iterator();
    while (secondIterator.hasNext()) {
      secondAsList.add(secondIterator.next());
    }

    List<T> result = new ArrayList<>();
    Iterator<T> iterator = source.iterator();
    while (iterator.hasNext()) {
      T value = iterator.next();
      if (!secondAsList.contains(value)) {
        result.add(value);
      }
    }
    return result;
  }


}

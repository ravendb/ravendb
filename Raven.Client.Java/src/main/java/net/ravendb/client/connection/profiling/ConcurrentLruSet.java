package net.ravendb.client.connection.profiling;


import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import net.ravendb.abstractions.basic.CollectionUtils;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Function1;


public class ConcurrentLruSet<T> {
  private final int maxCapacity;
  private final Action1<T> onDrop;
  private AtomicReference<List<T>> items = new AtomicReference<List<T>>(new LinkedList<T>());

  public ConcurrentLruSet(int maxCapacity, Action1<T> onDrop) {
    this.maxCapacity = maxCapacity;
    this.onDrop = onDrop;
  }

  public T firstOrDefault(Function1<T, Boolean> predicate) {
    return CollectionUtils.firstOrDefault(items.get(), predicate);
  }

  public void push(T item) {
    do {
      List<T> current = items.get();
      List<T> newList = new LinkedList<>(current);

      // this ensures the item is at the head of the list
      newList.remove(item);
      newList.add(item);

      T firstElement = null;
      if (newList.size() > maxCapacity)
      {
        firstElement = newList.get(0);
        newList.remove(0);
      }

      if (!items.compareAndSet(current, newList)) {
        continue;
      }

      if (onDrop != null && firstElement != null)
        onDrop.apply(firstElement);

      return;
    } while (true);
  }

  public void clear() {
    items.get().clear();
  }

  public void clearHalf() {
    do {
      List<T> current  = items.get();

      int halfIndex = current.size() / 2;

      List<T> newList = new LinkedList<>(current.subList(halfIndex, current.size()));
      if (!items.compareAndSet(current, newList)) {
        continue;
      }

      if (onDrop != null && halfIndex > 0) {
        for (T item: current.subList(0, halfIndex)) {
          onDrop.apply(item);
        }
      }
      return ;
    } while (true);
  }

  public void remove(T val) {
    items.get().remove(val);
  }


}
